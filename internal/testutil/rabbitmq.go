package testutil

import (
	"context"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

var (
	rmqOnce sync.Once
	rmqURL  string
	rmqErr  error
)

// RabbitMQ returns a connection to a RabbitMQ container that is shared by
// every test in the process. The connection is closed when the test ends.
// Callers own cleanup of the topology they declare, so tests using it must
// not run in parallel. Skipped with -short.
func RabbitMQ(t *testing.T) *amqp.Connection {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
	rmqOnce.Do(func() { rmqURL, rmqErr = startRabbitMQ() })
	if rmqErr != nil {
		t.Fatalf("start rabbitmq: %v", rmqErr)
	}

	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		t.Fatalf("connect rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func startRabbitMQ() (string, error) {
	ctx := context.Background()
	// The container is removed by testcontainers' reaper when the test binary exits.
	c, err := tcrabbit.Run(ctx, "rabbitmq:3.13-management-alpine")
	if err != nil {
		return "", err
	}
	return c.AmqpURL(ctx)
}
