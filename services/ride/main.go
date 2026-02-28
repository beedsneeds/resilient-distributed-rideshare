package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	// "os"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type rideServiceServer struct {
	ridepb.UnimplementedRideServiceServer
	// shared connection pool
	queries *ridedata.Queries
}

func sqlcRidetoProtoRide(r ridedata.Ride) *ridepb.Ride {
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
	var requestedAt *timestamppb.Timestamp
	if r.RequestedAt.Valid {
		requestedAt = timestamppb.New(r.RequestedAt.Time)
	}

	rideIDStr := uuid.UUID(r.ID.Bytes).String()
	riderIDstr := uuid.UUID(r.RiderID.Bytes).String()
	driverIDstr := uuid.UUID(r.DriverID.Bytes).String()
	status := rideStatusToProto[r.RideStatus]

	return &ridepb.Ride{
		Id:          &rideIDStr,
		RiderId:     &riderIDstr,
		DriverId:    &driverIDstr,
		RideStatus:  &status,
		RequestedAt: requestedAt,
	}
}

func (s *rideServiceServer) RequestRide(ctx context.Context, request *ridepb.RequestRideRequest) (*ridepb.RequestRideResponse, error) {
	driver, err := s.queries.GetRandomAvailableDriver(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}
	log.Printf("Driver: %v\n", driver)

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
		log.Fatalf("CreateRide failed: %v", err)
	}

	return &ridepb.RequestRideResponse{
		Ride: sqlcRidetoProtoRide(newRide),
	}, nil
}

func newServer() (*rideServiceServer, *pgx.Conn) {
	databaseURL := "postgres://postgres:postgres@ride-db:5432/ride_db"
	dbconn, err := pgx.Connect(context.Background(), databaseURL)
	// dbconn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	return &rideServiceServer{queries: ridedata.New(dbconn)}, dbconn
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
	rideServer, dbconn := newServer()
	defer dbconn.Close(context.Background())

	ridepb.RegisterRideServiceServer(grpcServer, rideServer)
	log.Printf("ride-service listening on port %d", *port)
	grpcServer.Serve(lis)

}
