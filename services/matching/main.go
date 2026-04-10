package main

import (
	"context"
	"errors"
	"time"

	"flag"
	"fmt"
	"log"

	"net"

	"encoding/json"

	"golang.org/x/sync/errgroup"

	"github.com/beedsneeds/resilient-distributed-rideshare/faultinject"
	matchingdata "github.com/beedsneeds/resilient-distributed-rideshare/services/matching/data"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	pgxpool    *pgxpool.Pool
	messages   *redis.Client
	rs         *redsync.Redsync
	rideClient ridepb.RideServiceClient // Don't currently use it as everything is event-driven. Leaving it here for future expansion
}

var errAlreadyMatched = errors.New("already matched")
var errTryNext = errors.New("try next driver")

// tryDriver attempts to match a single driver to a ride. Extracted into its own function to make use of defer
// Returns errAlreadyMatched if the ride was already matched (stop iterating),
// errTryNext if this driver couldn't be used (try the next one),
// or a real error if something unrecoverable happened.
func tryDriver(s matchingServiceServer, ctx context.Context, rideID uuid.UUID, driver matchingdata.Driver) (matchingdata.Driver, error) {
	payload, err := json.Marshal(map[string]string{"driverID": driver.ID.String()})
	if err != nil {
		return matchingdata.Driver{}, fmt.Errorf("marshal payload: %v", err)
	}

	mutex := s.rs.NewMutex(
		fmt.Sprintf("mutex-driver-%v", driver.ID.String()),
		redsync.WithExpiry(30*time.Second),
	)
	if err := mutex.Lock(); err != nil {
		log.Printf("Lock acquisition error: %v", err)
		return matchingdata.Driver{}, errTryNext
	}
	defer mutex.Unlock()

	// We assume driver is given 10s to accept/reject the ride
	// For simplicity, driver will always accept
	time.Sleep(3 * time.Second)

	// Begin Transaction: deduplicate, Update driver status, write to outbox
	tx, err := s.pgxpool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return matchingdata.Driver{}, fmt.Errorf("Could not create db transaction: %v", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	qtx := s.queries.WithTx(tx)

	// Deduplication
	_, err = qtx.CreateDedupEntry(ctx, matchingdata.CreateDedupEntryParams{
		RideID: pgtype.UUID{Bytes: rideID, Valid: true},
		Stream: matchingdata.StreamRiderequested,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("ride may have already matched %s, skipping", rideID)
			return matchingdata.Driver{}, errAlreadyMatched
		}
		log.Printf("Deduplication table error: %v", err)
		return matchingdata.Driver{}, errTryNext // should I let it retry despite a db error?
	}

	// Update driver status to busy
	if err = qtx.UpdateDriverStatus(ctx, matchingdata.UpdateDriverStatusParams{
		ID:     pgtype.UUID{Bytes: driver.ID.Bytes, Valid: true},
		Status: matchingdata.DriverstatusBusy,
	}); err != nil {
		return matchingdata.Driver{}, fmt.Errorf("UpdateDriverStatus failed: %v", err)
	}

	event, err := qtx.CreateOutboxEvent(ctx, matchingdata.CreateOutboxEventParams{
		RideID:  pgtype.UUID{Bytes: rideID, Valid: true},
		Stream:  matchingdata.StreamRideaccepted,
		Payload: payload,
	})
	if err != nil {
		return matchingdata.Driver{}, fmt.Errorf("CreateOutboxEvent failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return matchingdata.Driver{}, fmt.Errorf("tx commit failed: %v", err)
	}

	// Driver is matched, is busy and outbox written but processed message is not Acked - Lock should auto-expire and we should get an errAlreadyMatched
	faultinject.Injectf(faultinject.MatchingTryDriverAfterCommit, "rideID=%s outboxID=%s", event.RideID, event.ID)

	return driver, nil
}

// Lock to prevent double assignment of a single driver
// Dedup to prevent same ride from being matched twice
func matchDriver(ctx context.Context, s matchingServiceServer, rideID uuid.UUID) (matchingdata.Driver, error) {
	// Hardcoding max number of drivers to offer requests
	drivers, err := s.queries.GetNRandomAvailableDrivers(ctx, int32(5))
	if err != nil {
		return matchingdata.Driver{}, fmt.Errorf("GetNRandomAvailableDrivers failed: %v", err)
	}
	if len(drivers) == 0 {
		log.Printf("Ran out of drivers, resetting state")
		s.queries.ResetAllDriversToAvailable(ctx)
		drivers, err = s.queries.GetNRandomAvailableDrivers(ctx, int32(5))
		if err != nil || len(drivers) == 0 {
			return matchingdata.Driver{}, fmt.Errorf("GetNRandomAvailableDrivers failed after reset: no available drivers")
		}
	}

	for _, driver := range drivers {
		if driver.Status != "available" {
			continue
		}
		matched, err := tryDriver(s, ctx, rideID, driver)
		if errors.Is(err, errAlreadyMatched) {
			return matchingdata.Driver{}, errAlreadyMatched
		}
		if errors.Is(err, errTryNext) {
			continue
		}
		if err != nil {
			return matchingdata.Driver{}, err
		}
		return matched, nil
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
		if ctx.Err() != nil {
			log.Printf("context error propagated")
			return ctx.Err()
		}
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
			Block:    5 * time.Second,
		}).Result()
		if err == redis.Nil {
			// block timed out, no messages
			log.Printf("[%s] [ride.requested] No new messages \n", time.Now().Format("15:04:05"))
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

		rideID, err := uuid.Parse(message.Values["rideID"].(string))
		if err != nil {
			log.Printf("invalid rideID UUID %s: %v", rideID, err)
			continue
		}
		// Deduplicate messages once cheaply before matchDriver does, so we don't have to acquire locks and open transactions
		// This is just an optimization, not a dedup guarantee
		_, err = s.queries.CheckDedupEntry(ctx, matchingdata.CheckDedupEntryParams{
			RideID: pgtype.UUID{Bytes: rideID, Valid: true},
			Stream: matchingdata.StreamRiderequested,
		})
		if err == nil {
			// Ack and skip this message since duplicate entry found. Fresh entries get a pgx.ErrNoRows
			log.Printf("duplicate ride %s, skipping", rideID)
			if err := s.messages.XAck(ctx, "ride.requested", consgroup, message.ID).Err(); err != nil {
				log.Printf("XAck failed for message %s: %v", message.ID, err)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("Deduplication table error: %v", err)
			checkBacklog = true
			continue
		}

		// Match driver currently doesn't use rideID but it ideally takes rider location while matching
		_, err = matchDriver(ctx, s, rideID)
		if err != nil {
			if errors.Is(err, errAlreadyMatched) {
				if err := s.messages.XAck(ctx, "ride.requested", consgroup, message.ID).Err(); err != nil {
					log.Printf("XAck failed for message %s: %v", message.ID, err)
				}
				lastID = message.ID
				continue
			} else if err.Error() == "Could not match" {
				// Switch back to backlog mode so the failed message (now in PEL) is retried on the next iteration instead of being skipped by ">".
				checkBacklog = true
				continue
			} else {
				return err
			}
		}

		// log.Printf("Driver %v accepted ride with ID %v \n", driver, rideID)

		if err := s.messages.XAck(ctx, "ride.requested", consgroup, message.ID).Err(); err != nil {
			log.Printf("XAck failed for message %s: %v", message.ID, err)
		} else {
			// This log message is grepped in faults.sh
			log.Printf("[ride.requested] Successfully processed message %s (rideID: %s) \n", message.ID, rideID)
		}
		lastID = message.ID
	}
}

func publishOutboxEvents(ctx context.Context, s matchingServiceServer) error {
	const outboxTimeOut = 15
	for {
		event, err := s.queries.ClaimOutboxEvent(ctx, outboxTimeOut)
		if err == pgx.ErrNoRows {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			return fmt.Errorf("Query GetOutboxRow failed: %v", err)
		}

		var p struct {
			DriverID string `json:"driverID"`
		}
		if event.Payload == nil {
			log.Printf("WARNING: No payload where expected")
			continue
		}
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			log.Printf("failed to unmarshal outbox payload: %v", err)
			continue
		}

		// Crash after an event is claimed but before its processed - this event should republish after outboxTimeOut seconds
		faultinject.Injectf(faultinject.MatchingOutboxAfterClaim, "rideID=%s outboxID=%s", event.RideID, event.ID)

		xargs := redis.XAddArgs{
			Stream: string(event.Stream),
			ID:     "*",
			Values: []string{
				"rideID", event.RideID.String(),
				"driverID", p.DriverID,
			},
		}
		err = s.messages.XAdd(ctx, &xargs).Err()
		if err != nil {
			log.Printf("failed to publish ride event: %v", err)
			continue
		}

		// Crash after message is published but outbox status is not updated, thus will be republished - tests consumer deduplication
		faultinject.Injectf(faultinject.MatchingOutboxAfterXAdd, "rideID=%s outboxID=%s", event.RideID, event.ID)

		if err = s.queries.SetOutboxPublished(ctx, event.ID); err != nil {
			log.Printf("Query SetOutboxPublished failed: %v", err)
		}
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
		pgxpool:    pgxpool,
		messages:   rdsconn,
		rs:         redsync.New(rspool),
		rideClient: client,
	}

	// gRPC server to serve health check probes
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	g, ctx := errgroup.WithContext(context.Background())

	// Readiness goroutine that maintains service status by pinging dependencies
	g.Go(func() error {
		healthy := true
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			err1 := s.pgxpool.Ping(ctx)
			err2 := s.messages.Ping(ctx).Err()
			if err1 == nil && err2 == nil {
				if !healthy {
					log.Printf("Service recovered: dependencies are healthy")
					healthy = true
				}
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_SERVING)
			} else {
				if healthy {
					log.Printf("Service degraded: postgres=%v redis=%v", err1, err2)
					healthy = false
				} else {
					log.Printf("Service still degraded: postgres=%v redis=%v", err1, err2)
				}
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			}
		}
	})

	g.Go(func() error {
		const consumer = "ride-1" // TODO don't hard code this
		log.Printf("Processing Ride Accepted Status Updates...")
		return processRideRequests(ctx, *s, consumer)
	})
	// // Consume messages
	// g.Go(func() error {
	// 	const consumer = "matching-1" // TODO don't hard code this
	// 	for {
	// 		log.Printf("Processing Ride Requests...")
	// 		if err := processRideRequests(ctx, *s, consumer); err != nil {
	// 			if ctx.Err() != nil {
	// 				// propagate to the errgroup only if the context is already cancelled
	// 				// else recover
	// 				return ctx.Err()
	// 			}
	// 			log.Printf("processRideRequests exited with error, restarting: %v", err)
	// 		}
	// 		select {
	// 		case <-ctx.Done():
	// 			return ctx.Err()
	// 		case <-time.After(time.Second):
	// 		}
	// 	}
	// })

	// Outbox publisher
	g.Go(func() error {
		for {
			log.Printf("Publishing New Rides...")
			if err := publishOutboxEvents(ctx, *s); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("publishOutboxEvents exited with error, restarting: %v", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	})

	// Serving gRPC requests
	g.Go(func() error {
		log.Printf("matching-service gRPC listening on port %d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("gRPC serve: %w", err)
		}
		return nil
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
