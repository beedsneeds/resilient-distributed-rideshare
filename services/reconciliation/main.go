package main

import (
	"context"
	"log"
	// "os"
	"time"

	matchingdata "github.com/beedsneeds/resilient-distributed-rideshare/services/matching/data"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type reconciler struct {
	rideQueries     *ridedata.Queries
	matchingQueries *matchingdata.Queries
	redis           *redis.Client
}

// Tunable: determine how old a state must be before its considered stale
const (
	duplicateRideThreshold  = 60
	staleRequestedThreshold = 60 // rides in "requested" status that have no stream event (in seconds)
	// // TODO update driver details in ride
	// // staleMatchingThreshold = 120 // rides stuck in matching (what about matched)
	// stalePendingThreshold = 60 * time.Second // message delivered to consumer (matching service) but not ACK'ed
	reconcileInterval = 30 * time.Second
)

// Duplicated rides: When more than one ride is created for the same person within duplicateRideThreshold seconds

// Orphaned rides: When a ride is written to Postgres but is never published to the Redis stream
// Occurs when the ride-service crashed between db write and redis XADD
func (r *reconciler) checkOrphanedRides(ctx context.Context) {
	rides, err := r.rideQueries.GetStaleRidesByStatus(ctx, ridedata.GetStaleRidesByStatusParams{
		RideStatus: ridedata.RidestatusRequested,
		Column2:    staleRequestedThreshold,
	})
	if err != nil {
		log.Printf("ERROR checking orphaned rides: %v", err)
		return
	}
	if len(rides) == 0 {
		log.Printf("Orphaned rides: none found")
		return
	}
	for _, ride := range rides {

		log.Printf("ORPHANED RIDE: id=%x status=%s requested_at=%v",
			ride.ID.Bytes, ride.RideStatus, ride.RequestedAt.Time)
		// TODO: republish to stream, or mark as failed
	}
	log.Printf("Orphaned rides: %d found", len(rides))

}

func (r *reconciler) reconciliate(ctx context.Context) {
	log.Printf("[RECONCILIATOR] Starting reconciliation")
	for {
		log.Printf("[RECONCILIATOR] ==  Running Checks  ==")
		r.checkOrphanedRides(ctx)
		log.Printf("[RECONCILIATOR] == Checks  Complete ==")

		time.Sleep(reconcileInterval)

	}
}

func main() {
	ctx := context.Background()
	RIDE_DATABASE_URL := "postgres://postgres:postgres@ride-db:5432/ride_db"
	MATCHING_DATABASE_URL := "postgres://postgres:postgres@matching-db:5432/matching_db"

	ridePool, err := pgxpool.New(ctx, RIDE_DATABASE_URL)
	if err != nil {
		log.Fatalf("Could not connect to ride-db: %v", err)
	}
	defer ridePool.Close()

	matchingPool, err := pgxpool.New(ctx, MATCHING_DATABASE_URL)
	if err != nil {
		log.Fatalf("Could not connect to matching-db: %v", err)
	}
	defer matchingPool.Close()

	rdsconn := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	defer rdsconn.Close()

	r := &reconciler{
		rideQueries:     ridedata.New(ridePool),
		matchingQueries: matchingdata.New(matchingPool),
		redis:           rdsconn,
	}

	r.reconciliate(ctx)
}
