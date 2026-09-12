package streaming

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Any failure that can't be fixed by retrying (e.g. a dependency is broken)
var ErrFatal = errors.New("fatal stream error")

// UUIDField pulls a UUID out of a stream message. A missing field arrives as a
// nil interface, so this must not use an unchecked type assertion.
//
// It returns a plain error on purpose: whether an unparseable field is worth
// dropping the message over is the caller's call, not this package's.
func UUIDField(msg redis.XMessage, field string) (uuid.UUID, error) {
	raw, ok := msg.Values[field].(string)
	if !ok {
		return uuid.UUID{}, fmt.Errorf("message %s has no %s field", msg.ID, field)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("message %s has invalid %s %q: %v", msg.ID, field, raw, err)
	}
	return id, nil
}

// handle's returned error controls what happens next:
//   - nil: the message is acked
//   - ErrFatal: Process stops and returns the error
//   - anything else: treated as transient. The message is left unacked (in PEL)
func Process(ctx context.Context, messages *redis.Client, stream, consgroup, consumer string, handle func(context.Context, redis.XMessage) error) error {
	checkBacklog := true
	lastID := "0"

	err := messages.XGroupCreateMkStream(ctx, stream, consgroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("XGroupCreateMkStream: %v", err)
	}

	for {
		if ctx.Err() != nil {
			log.Printf("context error propagated")
			return ctx.Err()
		}
		var currID string
		if checkBacklog {
			currID = lastID
		} else {
			currID = ">"
		}

		streams, err := messages.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consgroup,
			Consumer: consumer,
			Streams:  []string{stream, currID},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()
		if err == redis.Nil {
			// block timed out, no messages
			log.Printf("[%s] [%s] No new messages \n", time.Now().Format("15:04:05"), stream)
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
			if errors.Is(err, ErrFatal) {
				return err
			}
			log.Printf("handler error: %v", err)
			// Switch back to backlog mode so the failed message (now in PEL)
			// is retried on the next iteration instead of being skipped by ">".
			checkBacklog = true
			continue
		}

		if err := messages.XAck(ctx, stream, consgroup, message.ID).Err(); err != nil {
			log.Printf("XAck failed for message %s: %v", message.ID, err)
		} else {
			// Progress marker for humans watching the logs; faults.sh verifies via psql/redis-cli
			log.Printf("[%s] Successfully processed message %s \n", stream, message.ID)
		}
		lastID = message.ID
	}
}
