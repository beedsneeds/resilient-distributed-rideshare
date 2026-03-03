package main

// import (
// 	"context"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"net"

// 	"github.com/go-redsync/redsync/v4"
// 	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
// 	"github.com/google/uuid"
// 	"github.com/redis/go-redis/v9"
// )

func main() {
	// client := redis.NewClient(&redis.Options{
	// 	Addr: "redis:6379",
	// })
	// // not redigo
	// pool := goredis.NewPool(client)
	// // defer?

	// rs := redsync.New(pool)

	// mutex := rs.NewMutex("matching-mutex")

	// // TODO Get list of drivers

	// // Try to lock each candidate
	// // for driver in drivers:

	// // Acquire Lock
	// err := mutex.Lock()
	// if err != nil {
	// 	log.Fatalf("Lock acquistion error: %v", err)
	// }
	// // Update driver status to busy
	// // Publish driver accepted event

	// // Release Lock
	// ok, err := mutex.Unlock()
	// if !ok || !err != nil {
	// 	log.Fatalf("Lock release error: %v", err)
	// }

	// // If no driver found, matching failed. Do something

	// // Just a test
	// driver, err := s.queries.GetRandomAvailableDriver(ctx)
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	// }
	// log.Printf("Driver: %v\n", driver)
}
