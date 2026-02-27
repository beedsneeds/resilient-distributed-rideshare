package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
	riderdata "github.com/beedsneeds/resilient-distributed-rideshare/services/rider/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr         = flag.String("addr", "localhost:50051", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.example.com", "The server name used to verify the hostname returned by the TLS handshake")
)

func requestRide(client ridepb.RideServiceClient, riderId string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ride, err := client.RequestRide(ctx, &ridepb.RequestRideRequest{
		IdempotencyKey: &riderId,
		RiderId:        &riderId,
	})
	if err != nil {
		log.Fatalf("client.RequestRide failed: %v", err)
	}
	log.Printf("Request Complete")
	r := ride.Ride
	log.Printf("Ride ID: %s", *r.Id)

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

	DATABASE_URL := "postgres://postgres:postgres@rider-db:5432/rider_db"
	dbconn, err := pgx.Connect(context.Background(), DATABASE_URL)
	// dbconn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer dbconn.Close(context.Background())

	queries := riderdata.New(dbconn)

	// Get an available rider
	rider, err := queries.GetRandomAvailableRider(context.Background())
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	fmt.Printf("Rider: %v\n", rider)
	uuidVal, _ := uuid.FromBytes(rider.ID.Bytes[:])
	riderId := uuidVal.String()

	requestRide(client, riderId)

}
