package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/beedsneeds/resilient-distributed-rideshare/faultinject"
	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type rideServiceServer struct {
	ridepb.UnimplementedRideServiceServer
	pool     *pgxpool.Pool
	queries  *ridedata.Queries
	messages *redis.Client
}

func (s *rideServiceServer) Close() {
	s.pool.Close()
	s.messages.Close()
}

var rideStatusToProto = map[ridedata.Ridestatus]ridepb.RideStatus{
	ridedata.RidestatusUnspecified: ridepb.RideStatus_RIDE_STATUS_UNSPECIFIED,
	ridedata.RidestatusRequested:   ridepb.RideStatus_RIDE_STATUS_REQUESTED,
	// ridedata.RidestatusMatching:    ridepb.RideStatus_RIDE_STATUS_MATCHING,
	// ridedata.RidestatusMatched:     ridepb.RideStatus_RIDE_STATUS_MATCHED,
	ridedata.RidestatusAccepted: ridepb.RideStatus_RIDE_STATUS_ACCEPTED,
	// ridedata.RidestatusInProgress:  ridepb.RideStatus_RIDE_STATUS_IN_PROGRESS,
	// ridedata.RidestatusCompleted:   ridepb.RideStatus_RIDE_STATUS_COMPLETED,
	// ridedata.RidestatusCancelled:   ridepb.RideStatus_RIDE_STATUS_CANCELLED,
	// ridedata.RidestatusFailed:      ridepb.RideStatus_RIDE_STATUS_FAILED,
}

func pgRidetoProtoRide(r ridedata.Ride) *ridepb.Ride {

	var requestedAt *timestamppb.Timestamp
	if r.RequestedAt.Valid {
		requestedAt = timestamppb.New(r.RequestedAt.Time)
	}

	rideIDStr := uuid.UUID(r.ID.Bytes).String()
	riderIDstr := uuid.UUID(r.RiderID.Bytes).String()
	rideStatus := rideStatusToProto[r.RideStatus]

	ride := &ridepb.Ride{
		Id:          &rideIDStr,
		RiderId:     &riderIDstr,
		RideStatus:  &rideStatus,
		RequestedAt: requestedAt,
	}
	if r.DriverID.Valid {
		driverIDstr := uuid.UUID(r.DriverID.Bytes).String()
		ride.DriverId = &driverIDstr
	}
	return ride
}

func (s *rideServiceServer) RequestRide(ctx context.Context, request *ridepb.RequestRideRequest) (*ridepb.RequestRideResponse, error) {
	newRideID := uuid.New()

	riderID, err := uuid.Parse(request.GetRiderId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid rider ID: %v", err)
	}
	idempotencyKey, err := uuid.Parse(request.GetIdempotencyKey())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid idempotency ID: %v", err)
	}

	// Atomically publish to outbox table while creating ride
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not create db transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	qtx := s.queries.WithTx(tx)

	// Deduplication
	rideID, err := qtx.CreateReqDedupEntry(ctx, pgtype.UUID{Bytes: idempotencyKey, Valid: true})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateReqDedupEntry failed: %v", err)
	}
	if rideID.Valid {
		log.Printf("ride already requested with ID %s, skipping", rideID)
		ride, err := qtx.GetRide(ctx, rideID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "GetRide failed: %v", err)
		}
		return &ridepb.RequestRideResponse{Ride: pgRidetoProtoRide(ride)}, nil
	}

	newRide, err := qtx.CreateRide(ctx, ridedata.CreateRideParams{
		ID:      pgtype.UUID{Bytes: newRideID, Valid: true},
		RiderID: pgtype.UUID{Bytes: riderID, Valid: true},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRide failed: %v", err)
	}

	event, err := qtx.CreateOutboxEvent(ctx, ridedata.CreateOutboxEventParams{
		RideID: pgtype.UUID{Bytes: newRideID, Valid: true},
		Stream: ridedata.StreamRiderequested,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateOutboxEvent failed: %v", err)
	}

	if err = qtx.InsertRedDedupRide(ctx, ridedata.InsertRedDedupRideParams{
		Idempkey: pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		RideID:   pgtype.UUID{Bytes: newRideID, Valid: true},
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "InsertRedDedupRide failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit failed: %v", err)
	}

	// Crash results in another ride being created since rider did not receive a response - Tests RequestRide idempotency on retries
	faultinject.Injectf(faultinject.RideRequestAfterCommit, "rideID=%s outboxID=%s", event.RideID, event.ID)

	return &ridepb.RequestRideResponse{
		Ride: pgRidetoProtoRide(newRide),
	}, nil
}

func publishOutboxEvents(ctx context.Context, s rideServiceServer) error {
	const outboxTimeOut = 15
	// // This is reused in reconciler/main.go
	for {
		event, err := s.queries.ClaimOutboxEvent(ctx, outboxTimeOut)
		if err == pgx.ErrNoRows {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			return fmt.Errorf("Query GetOutboxRow failed: %v", err)
		}

		// Crash after an event is claimed but before its processed - this event should republish after outboxTimeOut seconds
		faultinject.Injectf(faultinject.RideOutboxAfterClaim, "rideID=%s outboxID=%s", event.RideID, event.ID)

		xargs := redis.XAddArgs{
			Stream: string(event.Stream),
			ID:     "*",
			Values: []string{"rideID", event.RideID.String()},
		}
		err = s.messages.XAdd(ctx, &xargs).Err()
		if err != nil {
			log.Printf("failed to publish ride event: %v", err)
			continue
		}

		// Crash after message is published but outbox status is not updated, thus will be republished - tests consumer deduplication
		faultinject.Injectf(faultinject.RideOutboxAfterXAdd, "rideID=%s outboxID=%s", event.RideID, event.ID)

		if err = s.queries.SetOutboxPublished(ctx, event.ID); err != nil {
			log.Printf("Query SetOutboxPublished failed: %v", err)
		}
	}
}

func newServer() (*rideServiceServer, error) {
	databaseURL := "postgres://postgres:postgres@ride-db:5432/ride_db"
	pool, err := pgxpool.New(context.Background(), databaseURL)
	// pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}
	rdsconn := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	return &rideServiceServer{pool: pool, queries: ridedata.New(pool), messages: rdsconn}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// https://github.com/grpc/grpc-go/blob/master/examples/route_guide/server/server.go#L123
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	rideServer, err := newServer()
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer rideServer.Close()
	ridepb.RegisterRideServiceServer(grpcServer, rideServer)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	g, ctx := errgroup.WithContext(context.Background())

	// Liveness ("") defaults to SERVING automatically
	// Readyness ("readiness") starts as NOT_SERVING. The goroutine handles it fully
	g.Go(func() error {
		for {
			// Using select because the for-while loop did not have a return path
			select {
			case <-ctx.Done():
				// Exits when another goroutine fails
				return nil
			case <-time.After(5 * time.Second):
			}
			err1 := rideServer.pool.Ping(ctx)
			err2 := rideServer.messages.Ping(ctx).Err()
			if err1 == nil && err2 == nil {
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_SERVING)
			} else {
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
				log.Printf("readiness check failed: postgres=%v redis=%v", err1, err2)
			}
		}
	})

	// Consume messages
	// g.Go(func() error {
	// 	const consumer = "ride-1" // TODO don't hard code this
	// 	log.Printf("Processing Ride Matching Status Updates...")
	// 	return processRideMatchingStatus(ctx, *rideServer, consumer)
	// })
	g.Go(func() error {
		const consumer = "ride-1" // TODO don't hard code this
		log.Printf("Processing Ride Accepted Status Updates...")
		return processRideAcceptedStatus(ctx, rideServer, consumer)
	})
	// Outbox publisher
	g.Go(func() error {
		log.Printf("Publishing New Rides...")
		return publishOutboxEvents(ctx, *rideServer)
	})

	// Serving gRPC requests
	g.Go(func() error {
		log.Printf("ride-service listening on port %d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("gRPC serve: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("%v", err)
	}

}

// Using an async call for matching and accepted. However, this might be useful for cancelled and other important synchronous states, so I'll leave it
// func (s *rideServiceServer) UpdateRideStatus(ctx context.Context, request *ridepb.UpdateRideStatusRequest) (*ridepb.UpdateRideStatusResponse, error) {

// 	rideID, err := uuid.Parse(request.GetRideId())
// 	if err != nil {
// 		return nil, status.Errorf(codes.InvalidArgument, "invalid rider ID: %v", err)
// 	}
// 	var ride ridedata.Ride
// 	var qErr error
// 	switch rideStatus := request.GetRideStatus(); rideStatus {
// 	case *ridepb.RideStatus_RIDE_STATUS_MATCHING.Enum():
// 		ride, qErr = s.queries.UpdateRideMatching(ctx, pgtype.UUID{Bytes: rideID, Valid: true})
// 	// case *ridepb.RideStatus_RIDE_STATUS_MATCHED.Enum():
// 	// 	ride, qErr = s.queries.UpdateRideMatched(ctx, pgtype.UUID{Bytes: rideID, Valid: true})
// 	case *ridepb.RideStatus_RIDE_STATUS_ACCEPTED.Enum():
// 		driverID, dErr := uuid.Parse(request.GetDriverId())
// 		if dErr != nil {
// 			return nil, status.Errorf(codes.InvalidArgument, "invalid driver ID: %v", dErr)
// 		}
// 		ride, qErr = s.queries.UpdateRideAccepted(ctx, ridedata.UpdateRideAcceptedParams{
// 			ID:       pgtype.UUID{Bytes: rideID, Valid: true},
// 			DriverID: pgtype.UUID{Bytes: driverID, Valid: true}})
// 	default:
// 		return nil, status.Errorf(codes.InvalidArgument, "unsupported ride status: %v", rideStatus)
// 	}

// 	if qErr != nil {
// 		return nil, status.Errorf(codes.Internal, "failed to update ride status: %v", qErr)
// 	}

// 	return &ridepb.UpdateRideStatusResponse{
// 		Ride: pgRidetoProtoRide(ride),
// 	}, nil
// }
