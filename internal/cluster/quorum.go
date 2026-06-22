package cluster

import (
	"context"
	"time"
)

// WaitForQuorum polls isr.ISR(leaderID, leaderOffset) every pollInterval until
// at least quorum replicas are in the ISR set, or until ctx is cancelled.
//
// Returns nil when quorum is satisfied.
// Returns ctx.Err() when the context deadline is exceeded or is cancelled.
//
// If pollInterval is zero or negative the default of 5 ms is used.
func WaitForQuorum(
	ctx context.Context,
	isr *ISRTracker,
	leaderID string,
	leaderOffset int64,
	quorum int,
	pollInterval time.Duration,
) error {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Millisecond
	}

	// Fast path: quorum already satisfied without blocking.
	if len(isr.ISR(leaderID, leaderOffset)) >= quorum {
		return nil
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(isr.ISR(leaderID, leaderOffset)) >= quorum {
				return nil
			}
		}
	}
}
