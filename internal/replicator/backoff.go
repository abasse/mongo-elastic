package replicator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"
)

// RetryConfig controls exponential backoff behaviour.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// withRetry executes op, retrying up to cfg.MaxRetries times with exponential
// backoff and ±10 % jitter on each interval.  The context is respected between
// attempts; if it is cancelled the function returns ctx.Err() immediately.
func withRetry(ctx context.Context, cfg RetryConfig, logger *slog.Logger, opName string, op func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = op()
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxRetries {
			break
		}

		delay := backoffDelay(cfg.InitialBackoff, cfg.MaxBackoff, attempt)
		logger.Warn("operation failed, will retry",
			"op", opName,
			"attempt", attempt+1,
			"maxRetries", cfg.MaxRetries,
			"backoff", delay.String(),
			"error", lastErr,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("%s failed after %d attempt(s): %w", opName, cfg.MaxRetries+1, lastErr)
}

// backoffDelay computes 2^attempt * initial, capped at max, with ±10 % jitter.
func backoffDelay(initial, max time.Duration, attempt int) time.Duration {
	exp := time.Duration(float64(initial) * math.Pow(2, float64(attempt)))
	if exp > max || exp <= 0 { // guard against overflow
		exp = max
	}
	// Add uniform jitter in [-10 %, +10 %] of the computed delay.
	jitterRange := float64(exp) * 0.10
	jitter := time.Duration((rand.Float64()*2 - 1) * jitterRange)
	d := exp + jitter
	if d < 0 {
		d = initial
	}
	return d
}
