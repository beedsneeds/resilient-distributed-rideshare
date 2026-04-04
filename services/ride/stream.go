package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ridedata "github.com/beedsneeds/resilient-distributed-rideshare/services/ride/data"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

type StreamHandler interface {
	Handle(ctx context.Context, msg redis.XMessage) error
}

// Implements Handle
type RideAcceptedHandler struct {
	queries *ridedata.Queries
}

func (h *RideAcceptedHandler) Handle(ctx context.Context, msg redis.XMessage) error {
	rideID, err := uuid.Parse(msg.Values["rideID"].(string))
	if err != nil {
		return fmt.Errorf("invalid rideID: %v", err)
	}
	driverID, err := uuid.Parse(msg.Values["driverID"].(string))
	if err != nil {
		return fmt.Errorf("invalid driverID: %v", err)
	}
	ride, err := h.queries.SetRideAccepted(ctx, ridedata.SetRideAcceptedParams{
		ID:       pgtype.UUID{Bytes: rideID, Valid: true},
		DriverID: pgtype.UUID{Bytes: driverID, Valid: true}})
	if err != nil {
		return fmt.Errorf("SetRideAccepted failed: %v", err)
	}
	log.Printf("message ID: %s rideID: %s accepted driver %s", msg.ID, rideID, ride.DriverID)
	return nil
}

func processRideAcceptedStatus(ctx context.Context, s rideServiceServer, consumer string) error {
	return processStream(ctx, s, "ride.accepted", "ride-group", consumer, &RideAcceptedHandler{queries: s.queries})
}

// Polymorphism but with the same flow as matching/main
func processStream(ctx context.Context, s rideServiceServer, stream, consgroup, consumer string, handler StreamHandler) error {
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

		if err := handler.Handle(ctx, message); err != nil {
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

// type RideMatchingHandler struct {
// 	queries *ridedata.Queries
// }

// func (h *RideMatchingHandler) Handle(ctx context.Context, msg redis.XMessage) error {
// 	rideID, err := uuid.Parse(msg.Values["rideID"].(string))
// 	if err != nil {
// 		return fmt.Errorf("invalid rideID: %v", err)
// 	}
// 	ride, err := h.queries.SetRideMatching(ctx, pgtype.UUID{Bytes: rideID, Valid: true})
// 	if err != nil {
// 		return fmt.Errorf("SetRideMatching failed: %v", err)
// 	}
// 	log.Printf("message ID: %s rideID: %s matched for rider %s", msg.ID, rideID, ride.RiderID)
// 	return nil
// }

// func processRideMatchingStatus(ctx context.Context, s rideServiceServer, consumer string) error {
// 	return processStream(ctx, s, "ride.matching", "ride-group", consumer, &RideMatchingHandler{queries: s.queries})
// }
