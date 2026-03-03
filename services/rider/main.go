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

var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

func requestRide(client ridepb.RideServiceClient, riderID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ride, err := client.RequestRide(ctx, &ridepb.RequestRideRequest{
		// TODO implement actual idempotency with redis
		IdempotencyKey: &riderID,
		RiderId:        &riderID,
	})
	if err != nil {
		return fmt.Errorf("RequestRide failed: %w", err)
	}
	log.Printf("Ride ID: %s", *ride.Ride.Id)
	return nil
}

func main() {
	flag.Parse()

	// https://github.com/grpc/grpc-go/blob/master/examples/route_guide/client/client.go
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()
	client := ridepb.NewRideServiceClient(conn)

	databaseURL := "postgres://postgres:postgres@rider-db:5432/rider_db"
	pgconn, err := pgx.Connect(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pgconn.Close(context.Background())

	queries := riderdata.New(pgconn)

	for i := 1; i <= 10; i++ {
		time.Sleep(time.Duration(i*300) * time.Millisecond)

		rider, err := queries.GetRandomAvailableRider(context.Background())
		if err != nil {
			log.Printf("GetRandomAvailableRider failed: %v", err)
			continue
		}
		fmt.Printf("Rider: %v\n", rider)

		riderID, err := uuid.FromBytes(rider.ID.Bytes[:])
		if err != nil {
			log.Printf("invalid rider UUID: %v", err)
			continue
		}

		err = requestRide(client, riderID.String())
		if err != nil {
			log.Printf("requestRide failed for rider %s: %v", riderID, err)
		}
	}
}
