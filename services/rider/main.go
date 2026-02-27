package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	riderdata "github.com/beedsneeds/resilient-distributed-rideshare/services/rider/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr = flag.String("addr", "ride-service:50051", "The server address in the format of host:port")
)

func requestRide(client ridepb.RideServiceClient, riderID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := client.RequestRide(ctx, &ridepb.RequestRideRequest{
		IdempotencyKey: &riderID,
		RiderId:        &riderID,
	})
	if err != nil {
		return fmt.Errorf("RequestRide failed: %w", err)
	}
	// r := ride.Ride
	// log.Printf("Ride ID: %s", *r.Id)

	return nil
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	// https://github.com/grpc/grpc-go/blob/master/examples/route_guide/client/client.go
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()
	client := ridepb.NewRideServiceClient(conn)

	databaseURL := "postgres://postgres:postgres@rider-db:5432/rider_db"
	dbconn, err := pgx.Connect(context.Background(), databaseURL)
	// dbconn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer dbconn.Close(context.Background())

	queries := riderdata.New(dbconn)

	for i := 0; i < 10; i++ {
		delay := i * 300
		time.Sleep(time.Duration(delay) * time.Millisecond)
		// Get an available rider
		rider, err := queries.GetRandomAvailableRider(context.Background())
		if err != nil {
			log.Fatalf("CreateRide failed: %v", err)
		}
		fmt.Printf("Rider: %v\n", rider)
		uuidVal, _ := uuid.FromBytes(rider.ID.Bytes[:])
		riderID := uuidVal.String()

		requestRide(client, riderID)

	}

}
