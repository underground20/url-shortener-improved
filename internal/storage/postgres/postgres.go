package postgres

import (
	"context"
	"errors"
	"fmt"
	"url-shortener/internal/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Storage struct {
	db  *pgx.Conn
	ctx context.Context
}

func NewStorage(connectionUrl string, ctx context.Context) (*Storage, error) {
	const op = "storage.postgres.NewStorage"
	conn, err := pgx.Connect(ctx, connectionUrl)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: conn, ctx: ctx}, nil
}

func (s *Storage) SaveURL(url, alias string) (int64, error) {
	const op = "storage.postgres.SaveURL"
	var id int64
	err := s.db.QueryRow(
		s.ctx,
		`INSERT INTO url (alias, url) VALUES ($1, $2)`,
		alias,
		url,
	).Scan(&id)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return 0, fmt.Errorf("%s: %w", op, storage.ErrUrlExists)
		}

		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.postgres.GetURL"
	var url string

	err := s.db.QueryRow(
		s.ctx,
		`SELECT url FROM url WHERE alias = $1`,
		alias,
	).Scan(&url)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", storage.ErrUrlNotFound
	}
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return url, nil
}
