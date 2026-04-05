package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	// "os"
	"time"

	matchingdata "github.com/beedsneeds/resilient-distributed-rideshare/services/matching/data"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type reconciler struct {
	rideQueries     *ridedata.Queries
	matchingQueries *matchingdata.Queries
	messages        *redis.Client
}

// Tunable: determine how old a state must be before its considered stale
const (
	// duplicateRideThreshold  = 60
	staleRequestedThreshold = 60 // rides in "requested" status that have no stream event (in seconds)
	// // TODO update driver details in ride
	// stalePendingThreshold = 60 * time.Second // message delivered to consumer (matching service) but not ACK'ed
	reconcileInterval = 30 * time.Second
)

// Duplicated rides: When more than one ride is created for the same person within duplicateRideThreshold seconds
// We move all but the earliest into a terminal state (failed)
// func (r *reconciler) checkDuplicatedRides(ctx context.Context) {
// 	rides, err := r.rideQueries.GetDuplicatedRides(ctx, duplicateRideThreshold)
// 	if err != nil {
// 		log.Printf("ERROR checking duplicated rides: %v", err)
// 		return
// 	}
// 	if len(rides) == 0 {
// 		log.Printf("Duplicate rides: none found")
// 		return
// 	}
// 	for _, ride := range rides {

// 	}

// }

// Orphaned Requested Rides: When its been staleRequestedThreshold seconds since a ride was created/requested
// Occurs when
// 1. Outbox relay hasn't published the stream yet - relay failure/ride service crash
// 2. Matching service crash - never picked up the message - the orchestrator must handle this since matching service manages the logic
// 3. Matching service did not match driver or is slow to do so (picked up the message but never Ack'ed - still in PEL) - same as #2
// 4. Outbox (matching) has been written but hasn't been published yet
// 5. Ride service crash - did not pick up the status change message - the orchestrator must handle this since ride service manages the logic
// x. Logic errors with deduplication, etc
// y. db failures with postgres and redis
func (r *reconciler) checkOrphanedRequestedRides(ctx context.Context) {
	rides, err := r.rideQueries.GetStaleRides(ctx, ridedata.GetStaleRidesParams{
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
	log.Printf("Orphaned rides: %d found", len(rides))

	rideIDs := make([]pgtype.UUID, len(rides))
	for i, ride := range rides {
		rideIDs[i] = ride.ID
	}

	// Case 1:
	unpublishedRideEvents, err := r.rideQueries.GetUnpublishedOutboxEvents(ctx, rideIDs)
	if err != nil {
		log.Printf("DB error: GetUnpublishedOutboxEvents: %v", err)
		return
	}
	unpublishedRideEventSet := make(map[pgtype.UUID]ridedata.Outbox, len(unpublishedRideEvents))
	for _, event := range unpublishedRideEvents {
		unpublishedRideEventSet[pgtype.UUID(event.RideID)] = event
	}

	// Case 4:
	unpublishedMatchingEvents, err := r.matchingQueries.GetUnpublishedOutboxEvents(ctx, rideIDs)
	if err != nil {
		log.Printf("DB error: GetUnpublishedOutboxEvents: %v", err)
		return
	}
	unpublishedMatchingEventSet := make(map[pgtype.UUID]matchingdata.Outbox, len(unpublishedMatchingEvents))
	for _, event := range unpublishedMatchingEvents {
		unpublishedMatchingEventSet[pgtype.UUID(event.RideID)] = event
	}

	for _, ride := range rides {
		// Case 1: all events that haven't been published yet by the relay
		if event, ok := unpublishedRideEventSet[ride.ID]; ok {
			err = r.messages.XAdd(ctx, &redis.XAddArgs{
				Stream: string(event.Stream),
				ID:     "*",
				Values: []string{"rideID", event.RideID.String()},
			}).Err()
			if err != nil {
				log.Printf("RECONCILE: XAdd failed: %v", err)
				continue
			}
			if err = r.rideQueries.SetOutboxPublished(ctx, event.ID); err != nil {
				log.Printf("RECONCILE: SetOutboxPublished failed: %v", err)
			}
		}
		// Case 3: partially resolved by orchestrator (if matching service did not match)
		// partially resolved by Acking rides that have a matched driver

		// Case 4:
		if event, ok := unpublishedMatchingEventSet[ride.ID]; ok {
			err = r.messages.XAdd(ctx, &redis.XAddArgs{
				Stream: string(event.Stream),
				ID:     "*",
				Values: []string{"rideID", event.RideID.String()},
			}).Err()
			if err != nil {
				log.Printf("RECONCILE: XAdd failed: %v", err)
				continue
			}
			if err = r.matchingQueries.SetOutboxPublished(ctx, event.ID); err != nil {
				log.Printf("RECONCILE: SetOutboxPublished failed: %v", err)
			}
		}
	}

}

// 	log.Printf("ORPHANED RIDE: id=%x status=%s requested_at=%v",
// 		ride.ID.Bytes, ride.RideStatus, ride.RequestedAt.Time)

// 	// xargs := redis.XAddArgs{
// 	// 	Stream: "ride.requested",
// 	// 	ID:     "*",
// 	// 	Values: []string{"rideID", ride.ID.String()},
// 	// 	// TODO idempotency with IDMP
// 	// }
// 	// err = r.messages.XAdd(ctx, &xargs).Err()
// 	// if err != nil {
// 	// 	log.Printf("failed to publish ride event: %v", err)
// 	// }

func (r *reconciler) reconcile(ctx context.Context) {
	log.Printf("[RECONCILER] Starting")
	for {
		select {
		case <-ctx.Done():
			log.Printf("[RECONCILER] shutting down")
			return
		case <-time.After(reconcileInterval):
		}
		log.Printf("[RECONCILER] == Running  Checks ==")
		r.checkOrphanedRequestedRides(ctx)
		log.Printf("[RECONCILER] == Checks Complete ==")
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
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
		messages:        rdsconn,
	}

	r.reconcile(ctx)
}
