package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
	pgconn   *pgx.Conn
	queries  *ridedata.Queries
	messages *redis.Client
}

func (s *rideServiceServer) Close() {
	s.pgconn.Close(context.Background())
	s.messages.Close()
}

var rideStatusToProto = map[ridedata.Ridestatus]ridepb.RideStatus{
	ridedata.RidestatusUnspecified: ridepb.RideStatus_RIDE_STATUS_UNSPECIFIED,
	ridedata.RidestatusRequested:   ridepb.RideStatus_RIDE_STATUS_REQUESTED,
	ridedata.RidestatusMatching:    ridepb.RideStatus_RIDE_STATUS_MATCHING,
	ridedata.RidestatusMatched:     ridepb.RideStatus_RIDE_STATUS_MATCHED,
	ridedata.RidestatusAccepted:    ridepb.RideStatus_RIDE_STATUS_ACCEPTED,
	ridedata.RidestatusInProgress:  ridepb.RideStatus_RIDE_STATUS_IN_PROGRESS,
	ridedata.RidestatusCompleted:   ridepb.RideStatus_RIDE_STATUS_COMPLETED,
	ridedata.RidestatusCancelled:   ridepb.RideStatus_RIDE_STATUS_CANCELLED,
	ridedata.RidestatusFailed:      ridepb.RideStatus_RIDE_STATUS_FAILED,
}

func sqlcRidetoProtoRide(r ridedata.Ride) *ridepb.Ride {

	var requestedAt *timestamppb.Timestamp
	if r.RequestedAt.Valid {
		requestedAt = timestamppb.New(r.RequestedAt.Time)
	}

	rideIDStr := uuid.UUID(r.ID.Bytes).String()
	riderIDstr := uuid.UUID(r.RiderID.Bytes).String()
	driverIDstr := uuid.UUID(r.DriverID.Bytes).String()
	rideStatus := rideStatusToProto[r.RideStatus]

	return &ridepb.Ride{
		Id:          &rideIDStr,
		RiderId:     &riderIDstr,
		DriverId:    &driverIDstr,
		RideStatus:  &rideStatus,
		RequestedAt: requestedAt,
	}
}

func (s *rideServiceServer) RequestRide(ctx context.Context, request *ridepb.RequestRideRequest) (*ridepb.RequestRideResponse, error) {
	newRideID := uuid.New()

	riderID, err := uuid.Parse(request.GetRiderId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid rider ID: %v", err)
	}

	newRide, err := s.queries.CreateRide(ctx, ridedata.CreateRideParams{
		ID:      pgtype.UUID{Bytes: newRideID, Valid: true},
		RiderID: pgtype.UUID{Bytes: riderID, Valid: true},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRide failed: %v", err)
	}

	// What if service dies here?

	// This is reused in reconciliation/main.go
	xargs := redis.XAddArgs{
		Stream: "ride.requested",
		ID:     "*",
		Values: []string{"rideID", newRideID.String()},
		// TODO idempotency with IDMP
	}
	err = s.messages.XAdd(ctx, &xargs).Err()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to publish ride event: %v", err)
	}

	return &ridepb.RequestRideResponse{
		Ride: sqlcRidetoProtoRide(newRide),
	}, nil
}

func newServer() (*rideServiceServer, error) {
	databaseURL := "postgres://postgres:postgres@ride-db:5432/ride_db"
	pgconn, err := pgx.Connect(context.Background(), databaseURL)
	// pgconn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}
	rdsconn := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	return &rideServiceServer{pgconn: pgconn, queries: ridedata.New(pgconn), messages: rdsconn}, nil
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

	// Liveness ("") defaults to SERVING automatically
	// Readyness ("readiness") starts as NOT_SERVING. The goroutine handles it fully
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err1 := rideServer.pgconn.Ping(context.Background())
			err2 := rideServer.messages.Ping(context.Background()).Err()
			if err1 == nil && err2 == nil {
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_SERVING)
			} else {
				healthServer.SetServingStatus("readiness", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
				log.Printf("readiness check failed: postgres=%v redis=%v", err1, err2)
			}
		}
	}()

	log.Printf("ride-service listening on port %d", *port)
	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

}
