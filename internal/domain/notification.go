// Package domain holds the notification model and its business rules.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel is the delivery channel of a notification.
type Channel string

// Supported channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// Valid reports whether c is a supported channel.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority orders notifications waiting in the same channel queue.
type Priority string

// Supported priorities.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Valid reports whether p is a supported priority.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// AMQP maps the priority to a RabbitMQ message priority (higher is served first).
func (p Priority) AMQP() uint8 {
	switch p {
	case PriorityHigh:
		return 3
	case PriorityLow:
		return 1
	default:
		return 2
	}
}

// Status is the lifecycle state of a notification.
type Status string

// Lifecycle states. See docs/DESIGN.md for the state machine.
const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// DefaultMaxAttempts is how many delivery attempts a notification gets.
const DefaultMaxAttempts = 5

// Notification is a single message to deliver to one recipient.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	IdempotencyKey    *string
	Channel           Channel
	Recipient         string
	Content           string
	Priority          Priority
	Status            Status
	Attempts          int
	MaxAttempts       int
	NextAttemptAt     time.Time
	ScheduledAt       *time.Time
	ProviderMessageID *string
	LastError         *string
	CorrelationID     string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
}

// CreateInput is a client request to create a notification.
type CreateInput struct {
	Channel        string
	Recipient      string
	Content        string
	Priority       string
	IdempotencyKey *string
	ScheduledAt    *time.Time
	TemplateID     *uuid.UUID
	Variables      map[string]string
}

// NewNotification builds a pending notification from an already validated input.
func NewNotification(in CreateInput, correlationID string, now time.Time) Notification {
	prio := Priority(in.Priority)
	if prio == "" {
		prio = PriorityNormal
	}
	next := now
	if in.ScheduledAt != nil {
		next = *in.ScheduledAt
	}
	return Notification{
		ID:             uuid.New(),
		IdempotencyKey: in.IdempotencyKey,
		Channel:        Channel(in.Channel),
		Recipient:      in.Recipient,
		Content:        in.Content,
		Priority:       prio,
		Status:         StatusPending,
		MaxAttempts:    DefaultMaxAttempts,
		NextAttemptAt:  next,
		ScheduledAt:    in.ScheduledAt,
		CorrelationID:  correlationID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}
