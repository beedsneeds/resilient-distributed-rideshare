package main

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"os"

// 	ridepb "github.com/beedsneeds/resilient-distributed-rideshare/proto/ride"
// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials"
// 	"google.golang.org/grpc/credentials/insecure"
// )

func main() {
// 	var opts []grpc.DialOption
// 	if *tls {
// 		// if *caFile == "" {
// 		// 	*caFile = data.Path("x509/ca_cert.pem")
// 		// }
// 		// creds, err := credentials.NewClientTLSFromFile(*caFile, *serverHostOverride)
// 		// if err != nil {
// 		// 	log.Fatalf("Failed to create TLS credentials: %v", err)
// 		// }
// 		// opts = append(opts, grpc.WithTransportCredentials(creds))
// 	} else {
// 		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	}

// 	conn, err := grpc.NewClient(*serverAddr, opts...)
// 	if err != nil {
// 		log.Fatalf("could not dial: %v", err)
// 	}
// 	defer conn.Close()

// 	client := ridepb.NewRideServiceClient(conn)

}
