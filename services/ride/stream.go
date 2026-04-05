package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/beedsneeds/resilient-distributed-rideshare/faultinject"
	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

// Closure
func processRideAcceptedStatus(ctx context.Context, s *rideServiceServer, consumer string) error {
	const stream, consgroup = "ride.accepted", "ride-group"
	return processStream(ctx, s, stream, consgroup, consumer, func(ctx context.Context, msg redis.XMessage) error {
		rideID, err := uuid.Parse(msg.Values["rideID"].(string))
		if err != nil {
			return fmt.Errorf("invalid rideID: %v", err)
		}
		driverID, err := uuid.Parse(msg.Values["driverID"].(string))
		if err != nil {
			return fmt.Errorf("invalid driverID: %v", err)
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

		log.Printf("message ID: %s rideID: %s accepted driver %s", msg.ID, rideID, ride.DriverID)
		return nil
	})
}

func processStream(ctx context.Context, s *rideServiceServer, stream, consgroup, consumer string, handle func(context.Context, redis.XMessage) error) error {
	checkBacklog := true
	lastID := "0"

	err := s.messages.XGroupCreateMkStream(ctx, stream, consgroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("XGroupCreateMkStream: %v", err)
	}

	for {
		var currID string
		if checkBacklog {
			currID = lastID
		} else {
			currID = ">"
		}
		streams, err := s.messages.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consgroup,
			Consumer: consumer,
			Streams:  []string{stream, currID},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()
		if err == redis.Nil {
			// block timed out, no messages
			log.Printf("[%s] [%s] No new messages", time.Now().Format("15:04:05"), stream)
			continue
		}
		if err != nil {
			log.Printf("XRead error: %v", err)
			continue
		}
		if len(streams[0].Messages) == 0 {
			// Start processing new messages
			checkBacklog = false
			log.Printf("finished backlog")
			continue
		}
		message := streams[0].Messages[0]

		if err := handle(ctx, message); err != nil {
			log.Printf("handler error: %v", err)
			// Switch back to backlog mode so the failed message (now in PEL)
			// is retried on the next iteration instead of being skipped by ">".
			checkBacklog = true
			continue
		}

		err = s.messages.XAck(ctx, stream, consgroup, message.ID).Err()
		if err != nil {
			log.Printf("XAck failed for message %s: %v", message.ID, err)
		} else {
			log.Printf("[%s] Successfully processed message %s", stream, message.ID)
		}
		lastID = message.ID
	}
}
