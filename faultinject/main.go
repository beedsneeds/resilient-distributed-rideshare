package faultinject

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

type Action string

const (
	ActionPanic Action = "panic"
	ActionExit  Action = "exit"
)

// Implemented injection points:
// Using consts to prevent misspellings
const (
	// Tests RequestRide idempotency on retry
	RideRequestAfterCommit = "ride.request.after_commit"
	// Recovery behavior: Rollsback transaction and message stays in PEL
	RideAcceptedBeforeCommit = "ride.accepted.before_commit"

	MatchingTryDriverAfterCommit = "matching.trydriver.after_commit"

	MatchingOutboxAfterClaim = "matching.outbox.after_claim"
	MatchingOutboxAfterXAdd  = "matching.outbox.after_xadd"
	RideOutboxAfterClaim     = "ride.outbox.after_claim"
	RideOutboxAfterXAdd      = "ride.outbox.after_xadd"
)

var knownPoints = map[string]struct{}{
	RideRequestAfterCommit:       {},
	MatchingOutboxAfterClaim:     {},
	MatchingOutboxAfterXAdd:      {},
	MatchingTryDriverAfterCommit: {},
	RideAcceptedBeforeCommit:     {},
	RideOutboxAfterClaim:         {},
	RideOutboxAfterXAdd:          {},
}

var (
	once     sync.Once
	registry map[string]Action // maps point name to action
)

// Parse env var once. Should be in the form point:action like matching.outbox.after_xadd:exit
func load() {
	registry = map[string]Action{}
	raw := os.Getenv("FAULT_INJECT")
	if raw == "" {
		return
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			log.Fatalf("Fault Injection: malformed entry %q (want point:action)", entry)
		}
		point, action := parts[0], Action(parts[1])
		if _, ok := knownPoints[point]; !ok {
			log.Fatalf("Fault Injection: unknown injection point %q", point)
		}
		if action != ActionPanic && action != ActionExit {
			log.Fatalf("Fault Injection: unknown action %q", action)
		}
		registry[point] = action
	}
	log.Printf("Fault Injection Enabled: %v", registry)
}

func Inject(point string) {
	once.Do(load)
	fire(point, "")
}

func Injectf(point, format string, args ...any) {
	once.Do(load)
	fire(point, fmt.Sprintf(format, args...))
}

func fire(point, ctx string) {
	action, ok := registry[point]
	if !ok {
		return
	}
	ctxSuffix := ""
	if ctx != "" {
		ctxSuffix = " (" + ctx + ")"
	}
	switch action {
	case ActionPanic:
		log.Printf("FAULT INJECT [%s]: panicking (%s)", point, ctxSuffix)
		panic("fault injected at " + point + ctxSuffix)
	case ActionExit:
		log.Printf("FAULT INJECT [%s]: os.Exit(1) (%s)", point, ctxSuffix)
		os.Exit(1)
	}
	// No default case so every invalid Inject point falls through
}
