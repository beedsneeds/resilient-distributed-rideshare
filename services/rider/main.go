package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	riderdata "github.com/beedsneeds/resilient-distributed-rideshare/services/rider/data"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

// Jitter is +-20% backoff
var retryPolicy = `{
	"methodConfig": [{
		"name": [{"service": "ride.RideService"}],

		"retryPolicy": {
			"MaxAttempts": 4,
			"InitialBackoff": ".01s",
			"MaxBackoff": "1s",
			"BackoffMultiplier": 2,
            "RetryableStatusCodes": ["UNAVAILABLE", "DEADLINE_EXCEEDED"]
		}
	}]
}`

func requestRide(client ridepb.RideServiceClient, riderID string) (*ridepb.RequestRideResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ride, err := client.RequestRide(ctx, &ridepb.RequestRideRequest{
		// TODO implement actual idempotency with redis
		IdempotencyKey: &riderID,
		RiderId:        &riderID,
	})
	if err != nil {
		return nil, fmt.Errorf("RequestRide failed: %w", err)
	}
	return ride, nil
}

func main() {
	flag.Parse()

	// https://github.com/grpc/grpc-go/blob/master/examples/route_guide/client/client.go
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(retryPolicy))
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()
	client := ridepb.NewRideServiceClient(conn)

	databaseURL := "postgres://postgres:postgres@rider-db:5432/rider_db"
	pgxpool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pgxpool.Close()

	queries := riderdata.New(pgxpool)

	for i := 1; i <= 30; i++ {
		time.Sleep(time.Duration(5) * time.Second)

		rider, err := queries.GetRandomAvailableRider(context.Background())
		if err != nil {
			log.Printf("GetRandomAvailableRider failed: %v", err)
			continue
		}

		// Health check before every request
		healthClient := healthpb.NewHealthClient(conn)
		healthCtx, healthCancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := healthClient.Check(healthCtx, &healthpb.HealthCheckRequest{
			Service: "readiness",
		})
		healthCancel() // Check() is a blocking call, so we can safely cancel without deferring
		if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
			log.Printf("ride-service not ready, skipping request")
			continue
		}

		ride, err := requestRide(client, rider.ID.String())
		if err != nil {
			log.Printf("requestRide failed for rider %s: %v", rider.Name, err)
			continue
		}
		fmt.Printf("Rider: %v\n", rider)
		fmt.Printf("Ride ID: %v\n", ride.Ride.Id)
	}
}
