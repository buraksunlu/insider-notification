package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// Collector periodically refreshes the gauges that describe system state:
// queue depth (RabbitMQ) and notifications per status (database). Only one
// process (the worker) runs it, so every value is reported exactly once.
type Collector struct {
	Metrics  *Metrics
	Depths   func(ctx context.Context) (map[string]int, error)
	Statuses func(ctx context.Context) (map[domain.Status]int64, error)
	Interval time.Duration
	Logger   *slog.Logger
}

// Run collects immediately and then every Interval until ctx is cancelled.
// A failing source is logged and its gauges keep their last known values.
func (c *Collector) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		c.collect(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *Collector) collect(ctx context.Context) {
	if c.Depths != nil {
		if d, err := c.Depths(ctx); err != nil {
			c.warn(ctx, "collect queue depth failed", err)
		} else {
			c.Metrics.SetQueueDepths(d)
		}
	}
	if c.Statuses != nil {
		if s, err := c.Statuses(ctx); err != nil {
			c.warn(ctx, "collect status counts failed", err)
		} else {
			c.Metrics.SetStatusCounts(s)
		}
	}
}

func (c *Collector) warn(ctx context.Context, msg string, err error) {
	if ctx.Err() == nil {
		c.Logger.WarnContext(ctx, msg, "error", err)
	}
}
