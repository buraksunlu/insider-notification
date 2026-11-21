package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// ErrTemplateNameTaken means a template with that name already exists.
var ErrTemplateNameTaken = errors.New("template name already exists")

const templateColumns = `id, name, channel, body, created_at`

// CreateTemplate stores t. A duplicate name is ErrTemplateNameTaken.
func (s *Store) CreateTemplate(ctx context.Context, t domain.Template) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO templates (`+templateColumns+`) VALUES ($1, $2, $3, $4, $5)`,
		t.ID, t.Name, t.Channel, t.Body, t.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return ErrTemplateNameTaken
	}
	return err
}

// GetTemplate returns one template by id.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	return scanTemplate(s.pool.QueryRow(ctx, `SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
}

// ListTemplates returns every template, sorted by name. Templates are a
// small, curated set, so the list is not paginated.
func (s *Store) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+templateColumns+` FROM templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Template{}
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanTemplate(row pgx.Row) (domain.Template, error) {
	var t domain.Template
	err := row.Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}
