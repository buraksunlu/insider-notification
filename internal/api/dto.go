package api

import (
	"time"

	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

type createRequest struct {
	Channel     string     `json:"channel"`
	Recipient   string     `json:"recipient"`
	Content     string     `json:"content"`
	Priority    string     `json:"priority"`
	ScheduledAt *time.Time `json:"scheduled_at"`
	// TemplateID and Variables replace Content: the template is rendered.
	TemplateID *string           `json:"template_id"`
	Variables  map[string]string `json:"variables"`
}

func (c createRequest) input() domain.CreateInput {
	return domain.CreateInput{
		Channel: c.Channel, Recipient: c.Recipient, Content: c.Content,
		Priority: c.Priority, ScheduledAt: c.ScheduledAt,
	}
}

type batchItem struct {
	createRequest
	IdempotencyKey *string `json:"idempotency_key"`
}

type batchRequest struct {
	Notifications []batchItem `json:"notifications"`
}

type notificationDTO struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batch_id,omitempty"`
	IdempotencyKey    *string    `json:"idempotency_key,omitempty"`
	Channel           string     `json:"channel"`
	Recipient         string     `json:"recipient"`
	Content           string     `json:"content"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"max_attempts"`
	ScheduledAt       *time.Time `json:"scheduled_at,omitempty"`
	NextAttemptAt     *time.Time `json:"next_attempt_at,omitempty"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	LastError         *string    `json:"last_error,omitempty"`
	CorrelationID     string     `json:"correlation_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SentAt            *time.Time `json:"sent_at,omitempty"`
	TemplateID        *uuid.UUID `json:"template_id,omitempty"`
}

func toDTO(n domain.Notification) notificationDTO {
	d := notificationDTO{
		ID: n.ID, BatchID: n.BatchID, IdempotencyKey: n.IdempotencyKey,
		Channel: string(n.Channel), Recipient: n.Recipient, Content: n.Content,
		Priority: string(n.Priority), Status: string(n.Status),
		Attempts: n.Attempts, MaxAttempts: n.MaxAttempts, ScheduledAt: n.ScheduledAt,
		ProviderMessageID: n.ProviderMessageID, LastError: n.LastError,
		CorrelationID: n.CorrelationID, CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt, SentAt: n.SentAt,
		TemplateID: n.TemplateID,
	}
	// next_attempt_at only means something while the notification waits.
	if n.Status == domain.StatusPending {
		next := n.NextAttemptAt
		d.NextAttemptAt = &next
	}
	return d
}

func toDTOs(ns []domain.Notification) []notificationDTO {
	out := make([]notificationDTO, len(ns)) // never nil: empty lists encode as []
	for i, n := range ns {
		out[i] = toDTO(n)
	}
	return out
}

type listResponse struct {
	Data       []notificationDTO `json:"data"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type batchCreatedResponse struct {
	BatchID uuid.UUID   `json:"batch_id"`
	IDs     []uuid.UUID `json:"ids"`
}

type batchResponse struct {
	BatchID    uuid.UUID         `json:"batch_id"`
	Total      int               `json:"total"`
	Counts     map[string]int    `json:"counts"`
	CreatedAt  time.Time         `json:"created_at"`
	Data       []notificationDTO `json:"data"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type createTemplateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

type templateDTO struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"created_at"`
}

func toTemplateDTO(t domain.Template) templateDTO {
	return templateDTO{
		ID: t.ID, Name: t.Name, Channel: string(t.Channel), Body: t.Body,
		Variables: t.Variables(), CreatedAt: t.CreatedAt,
	}
}

type templateListResponse struct {
	Data []templateDTO `json:"data"`
}
