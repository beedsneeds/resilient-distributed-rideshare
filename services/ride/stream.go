package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/beedsneeds/resilient-distributed-rideshare/faultinject"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/beedsneeds/resilient-distributed-rideshare/streaming"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

// Closure
func processRideAcceptedStatus(ctx context.Context, s *rideServiceServer, consumer string) error {
	const stream, consgroup = "ride.accepted", "ride-group"
	return streaming.Process(ctx, s.messages, stream, consgroup, consumer, func(ctx context.Context, msg redis.XMessage) error {
		rideID, err := streaming.UUIDField(msg, "rideID")
		if err != nil {
			log.Printf("[%s] DROPPING %s: %v", stream, msg.ID, err)
			return nil // nil acks it
		}
		driverID, err := streaming.UUIDField(msg, "driverID")
		if err != nil {
			log.Printf("[%s] DROPPING %s: %v", stream, msg.ID, err)
			return nil // nil acks it
		}

		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("Could not create db transaction: %v", err)
		}
		defer tx.Rollback(ctx)

		qtx := s.queries.WithTx(tx)

		// Deduplication
		_, err = qtx.CreateDedupEntry(ctx, ridedata.CreateDedupEntryParams{
			RideID: pgtype.UUID{Bytes: rideID, Valid: true},
			Stream: ridedata.StreamRideaccepted,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				log.Printf("ride may have already matched %s, skipping", rideID)
				return nil
			}
			return fmt.Errorf("Deduplication table error: %v", err) // should I let it retry despite a db error?
		}

		ride, err := qtx.SetRideAccepted(ctx, ridedata.SetRideAcceptedParams{
			ID:       pgtype.UUID{Bytes: rideID, Valid: true},
			DriverID: pgtype.UUID{Bytes: driverID, Valid: true}})
		if err != nil {
			return fmt.Errorf("SetRideAccepted failed: %v", err)
		}

		// Crash rollsback status update transaction and message stays in PEL - Verifies deduplication table
		faultinject.Injectf(faultinject.RideAcceptedBeforeCommit, "rideID=%s msgID=%s", ride.ID, msg.ID)

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("tx commit failed: %v", err)
		}

		// log.Printf("message ID: %s rideID: %s accepted driver %s for rider %s", msg.ID, rideID, ride.DriverID, ride.RiderID)
		return nil
	})
}
