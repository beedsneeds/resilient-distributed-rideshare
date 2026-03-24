package main

import (
	"context"
	"time"

	"flag"
	"fmt"
	"log"

	"net"

	"golang.org/x/sync/errgroup"

	matchingdata "github.com/beedsneeds/resilient-distributed-rideshare/services/matching/data"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"

	// "github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	port       = flag.Int("port", 50053, "The server port")
	serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")
)

type matchingServiceServer struct {
	queries    *matchingdata.Queries
	messages   *redis.Client
	rs         *redsync.Redsync
	rideClient ridepb.RideServiceClient // Don't currently use it as everything is event-driven. Leaving it here for future expansion
}

func matchDriver(s matchingServiceServer) (matchingdata.Driver, error) {
	// Hardcoding max number of drivers to offer requests
	drivers, err := s.queries.GetNRandomAvailableDrivers(context.Background(), int32(5))
	if err != nil {
		return matchingdata.Driver{}, fmt.Errorf("GetNRandomAvailableDrivers failed: %v", err)
	}
	if len(drivers) == 0 {
		log.Printf("Ran out of drivers, resetting state")
		s.queries.ResetAllDriversToAvailable(context.Background())
		drivers, err = s.queries.GetNRandomAvailableDrivers(context.Background(), int32(5))
		if err != nil || len(drivers) == 0 {
			return matchingdata.Driver{}, fmt.Errorf("GetNRandomAvailableDrivers failed after reset: no available drivers")
		}
	}

	ctx := context.Background()
	for _, driver := range drivers {
		if driver.Status != "available" {
			continue
		}

		mutex := s.rs.NewMutex(
			fmt.Sprintf("mutex-driver-%v", driver.ID.String()),
			redsync.WithExpiry(30*time.Second),
		)
		// Try to acquire lock
		err := mutex.Lock()
		if err != nil {
			log.Printf("Lock acquistion error: %v", err)
			continue
		}

		// Update driver status to busy
		err = s.queries.UpdateDriverStatus(ctx, matchingdata.UpdateDriverStatusParams{
			ID:     pgtype.UUID{Bytes: driver.ID.Bytes, Valid: true},
			Status: matchingdata.DriverstatusBusy,
		})
		if err != nil {
			mutex.Unlock()
			return matchingdata.Driver{}, fmt.Errorf("UpdateDriverStatus failed: %v", err)
		}

		// We assume driver is given 10s to accept/reject the ride
		// For simplicity, driver will always accept
		time.Sleep(3 * time.Second)

		// TODO Publish driver accepted event or should it be a synchronous RPC?

		// Release Lock
		ok, err := mutex.Unlock()
		if !ok || err != nil {
			log.Fatalf("Lock release error: %v", err)
		}

		return driver, nil
		// If no driver found, matching failed. Do something
	}
	// Warning: This error message is coupled to retry logic in processRideRequests. Change both or none.
	return matchingdata.Driver{}, fmt.Errorf("Could not match")
}

func processRideRequests(ctx context.Context, s matchingServiceServer, consumer string) error {
	checkBacklog := true
	lastID := "0"
	const consgroup = "matching-group"

	// Only create streams that will be consumed here
	err := s.messages.XGroupCreateMkStream(ctx, "ride.requested", consgroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("XGroupCreateMkStream: %v", err)
	}

	for {
		var currID string
		if checkBacklog {
			currID = lastID
		} else {
			currID = ">"
		}

		streams, err := s.messages.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consgroup,
			Consumer: consumer,
			Streams:  []string{"ride.requested", currID},
			Count:    1,
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			// block timed out, no messages
			fmt.Printf("\r[%s] [ride.requested] No new messages", time.Now().Format("15:04:05"))

			continue
		}
		if err != nil {
			log.Printf("XRead error: %v", err)
			continue
		}
		if len(streams[0].Messages) == 0 {
			// Start processesing new messages
			checkBacklog = false
			log.Printf("finished backlog")
			continue
		}
		message := streams[0].Messages[0]
		rideID := message.Values["rideID"].(string)
		log.Printf("\nmessage ID: %s", message.ID)

		// Publish an update to the Ride Status
		xargs := redis.XAddArgs{
			Stream: "ride.matching",
			ID:     "*",
			Values: []string{"rideID", rideID},
		}
		err = s.messages.XAdd(ctx, &xargs).Err()
		if err != nil {
			log.Printf("updateRideStatus MATCHING failed for ride %s: %v", rideID, err)
		}

		// Match driver currently doesn't use rideID but it ideally takes rider location while matching
		driver, err := matchDriver(s)
		if err != nil {
			if err.Error() == "Could not match" {
				continue
			} else {
				return err
			}
		}

		// Publish an update to the Ride Status
		log.Printf("\nDriver %v accepted ride with ID %v", driver, rideID)
		xargs = redis.XAddArgs{
			Stream: "ride.accepted",
			ID:     "*",
			Values: []string{"rideID", rideID, "driverID", driver.ID.String()},
		}
		err = s.messages.XAdd(ctx, &xargs).Err()
		if err != nil {
			log.Printf("updateRideStatus ACCEPTED failed for ride %s: %v", rideID, err)
		}

		// Possible failure scenario

		if err := s.messages.XAck(ctx, "ride.requested", consgroup, message.ID).Err(); err != nil {
			log.Printf("XAck failed for message %s: %v", message.ID, err)
		}
		lastID = message.ID
	}
}

func main() {
	flag.Parse()
	// Connections
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()
	client := ridepb.NewRideServiceClient(conn)

	databaseURL := "postgres://postgres:postgres@matching-db:5432/matching_db"
	pgxpool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pgxpool.Close()

	rdsconn := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})
	defer rdsconn.Close()

	// Redsync
	rs := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	defer rs.Close()
	// not redigo
	rspool := goredis.NewPool(rs)

	s := &matchingServiceServer{
		queries:    matchingdata.New(pgxpool),
		messages:   rdsconn,
		rs:         redsync.New(rspool),
		rideClient: client,
	}

	// gRPC server to serve health check probes
	// TODO: when I implement synchronous RPCs that are served in this gRPC server, use errgroup to manage both goroutine failures
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	// Readiness goroutine that maintains service status by pinging dependencies
	go func() {
		time.Sleep(5 * time.Second)
		err1 := pgxpool.Ping(context.Background())          // TODO: Should I add it to server struct??
		err2 := s.messages.Ping(context.Background()).Err() // TODO: conversely, should I ping rdsconn instead?
		if err1 == nil && err2 == nil {
			healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_SERVING)
		} else {
			healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			log.Printf("readiness check failed: postgres=%v redis=%v", err1, err2)
		}
	}()

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		log.Printf("matching-service gRPC listening on port %d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("gRPC serve: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		const consumer = "matching-1" // TODO don't hard code this
		log.Printf("Processing Ride Requests...")
		return processRideRequests(ctx, *s, consumer)
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("%v", err)
	}

}

// See ride/main.go UpdateRideStatus
// // Optional Parameter: driverID is not required (pass "" instead) unless Status is 'Accepted'
// func updateRideStatus(client ridepb.RideServiceClient, rideID string, status ridepb.RideStatus, driverID string) (*ridepb.UpdateRideStatusResponse, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	req := &ridepb.UpdateRideStatusRequest{
// 		IdempotencyKey: &rideID,
// 		RideId:         &rideID,
// 		RideStatus:     &status,
// 	}
// 	if driverID != "" {
// 		req.DriverId = &driverID
// 	}

// 	ride, err := client.UpdateRideStatus(ctx, req)
// 	if err != nil {
// 		return nil, fmt.Errorf("updateRideStatus failed: %w", err)
// 	}
// 	return ride, nil
// }
// _, err = updateRideStatus(s.rideClient, rideID, ridepb.RideStatus_RIDE_STATUS_MATCHING, "")
// if err != nil {
// 	log.Printf("updateRideStatus MATCHING failed for ride %s: %v", rideID, err)
// }
// _, err = updateRideStatus(s.rideClient, rideID, ridepb.RideStatus_RIDE_STATUS_ACCEPTED, driver.ID.String())
// if err != nil {
// 	log.Printf("updateRideStatus ACCEPTED failed for ride %s: %v", rideID, err)
// }
