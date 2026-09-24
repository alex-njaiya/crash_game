package round

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

type Repository interface {
	InsertRound(ctx context.Context, round Round) error
	MarkRunning(ctx context.Context, roundId uuid.UUID, runningStartedAt time.Time) error
	MarkCrashed(ctx context.Context, roundId uuid.UUID, crashedAt time.Time) error
	GetRoundById(ctx context.Context, id uuid.UUID) (*Round, error)
	GetRecentRounds(ctx context.Context, limit int) ([]*Round, error)
	GetRoundsBetween(ctx context.Context, from, to time.Time) ([]*Round, error)
}

func NewPostgreRepo(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool: pool,
	}
}

func (r *PostgresRepository) InsertRound(ctx context.Context, round Round) error {
	query := `INSERT INTO rounds (id, state, server_seed, client_seed, server_seed_hash, nonce, house_edge, crashpoint, started_at, running_started_at, crashed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	_, err := r.pool.Exec(ctx, query,
		round.ID,
		round.State,
		round.ServerSeed,
		round.ClientSeed,
		round.ServerSeedHash,
		round.Nonce,
		round.HouseEdge,
		round.CrashPoint,
		round.StartedAt,
		round.RunningStartedAt,
		round.CrashedAt,
	)

	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresRepository) GetRoundById(ctx context.Context, roundId uuid.UUID) (*Round, error) {
	round := new(Round)

	query := `SELECT id, state, server_seed, client_seed, server_seed_hash, nonce, house_edge, crashpoint, started_at, running_started_at, crashed_at FROM rounds WHERE id = $1`

	err := r.pool.QueryRow(ctx, query, roundId).Scan(
		&round.ID,
		&round.State,
		&round.ServerSeed,
		&round.ClientSeed,
		&round.ServerSeedHash,
		&round.Nonce,
		&round.HouseEdge,
		&round.CrashPoint,
		&round.StartedAt,
		&round.RunningStartedAt,
		&round.CrashedAt,
	)

	if err != nil {
		return nil, err
	}

	return round, nil
}

func (r *PostgresRepository) GetRecentRounds(ctx context.Context, limit int) ([]*Round, error) {
	query := `SELECT id, state, server_seed, client_seed, server_seed_hash, nonce, house_edge, crashpoint, started_at, running_started_at, crashed_at FROM rounds ORDER BY started_at DESC LIMIT $1`

	rows, err := r.pool.Query(ctx, query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rounds []*Round

	for rows.Next() {
		singleRound := new(Round)

		err := rows.Scan(
			&singleRound.ID,
			&singleRound.State,
			&singleRound.ServerSeed,
			&singleRound.ClientSeed,
			&singleRound.ServerSeedHash,
			&singleRound.Nonce,
			&singleRound.HouseEdge,
			&singleRound.CrashPoint,
			&singleRound.StartedAt,
			&singleRound.RunningStartedAt,
			&singleRound.CrashedAt,
		)

		if err != nil {
			return nil, err
		}

		rounds = append(rounds, singleRound)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rounds, nil
}

func (r *PostgresRepository) GetRoundsBetween(ctx context.Context, from, to time.Time) ([]*Round, error) {
	query := `SELECT id, state, server_seed, client_seed, server_seed_hash, nonce, house_edge, crashpoint, started_at, running_started_at, crashed_at FROM rounds WHERE started_at >= $1 AND started_at <= $2 ORDER BY started_at ASC`

	rows, err := r.pool.Query(ctx, query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rounds []*Round

	for rows.Next() {
		singleRound := new(Round)

		err := rows.Scan(
			&singleRound.ID,
			&singleRound.State,
			&singleRound.ServerSeed,
			&singleRound.ClientSeed,
			&singleRound.ServerSeedHash,
			&singleRound.Nonce,
			&singleRound.HouseEdge,
			&singleRound.CrashPoint,
			&singleRound.StartedAt,
			&singleRound.RunningStartedAt,
			&singleRound.CrashedAt,
		)

		if err != nil {
			return nil, err
		}

		rounds = append(rounds, singleRound)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rounds, nil
}

func (r *PostgresRepository) MarkRunning(ctx context.Context, roundId uuid.UUID, runningStartedAt time.Time) error {
	query := `UPDATE rounds SET running_started_at = $1 WHERE id = $2`

	_, err := r.pool.Exec(ctx, query, runningStartedAt, roundId)

	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresRepository) MarkCrashed(ctx context.Context, roundId uuid.UUID, crashedAt time.Time) error {
	query := `UPDATE rounds SET crashed_at = $1 WHERE id = $2`

	_, err := r.pool.Exec(ctx, query, crashedAt, roundId)

	if err != nil {
		return err
	}

	return nil
}
