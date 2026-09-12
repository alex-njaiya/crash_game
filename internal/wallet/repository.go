package wallet

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

// inject an active transaction into a context
func contextWithKey(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}


func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return contextWithKey(ctx, tx)
}

// extract an active transaction from a context if exists
func TxtContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)

	return tx, ok
}

type Repository interface {
	CreateWallet(ctx context.Context, UserID uuid.UUID) (*Wallet, error)
	GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error)
	InsertLedgerEntry(ctx context.Context, entry LedgerEntry) error
	FindByIdempotencyKey(ctx context.Context, key string) (*LedgerEntry, error)
	LockWallet(ctx context.Context, walletID uuid.UUID) error
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool: pool,
	}
}

func (r *PostgresRepository) CreateWallet(ctx context.Context, UserID uuid.UUID) (*Wallet, error) {
	wallet := new(Wallet)

	query := `INSERT INTO wallets (user_id) VALUES ($1) RETURNING id, user_id, currency, created_at`

	// check for transaction in ctx
	if tx, ok := TxtContext(ctx); ok {
		err := tx.QueryRow(ctx, query, UserID).Scan(&wallet.ID, &wallet.UserID, &wallet.Currency, &wallet.CreatedAt)

		if err != nil {
			return nil, err
		}

		return wallet, nil
	}

	// fallback to the connection pool if no transaction
	err := r.pool.QueryRow(ctx, query, UserID).Scan(&wallet.ID, &wallet.UserID, &wallet.Currency, &wallet.CreatedAt)

	if err != nil {
		return nil, err
	}

	return wallet, nil
}

func (r *PostgresRepository) GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error) {
	var balance int64

	query := `SELECT balance FROM wallet_balances WHERE wallet_id = $1`

	if tx, ok := TxtContext(ctx); ok {
		err := tx.QueryRow(ctx, query, walletID).Scan(&balance)

		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}

		if err != nil {
			return 0, err
		}

		return balance, nil
	}

	err := r.pool.QueryRow(ctx, query, walletID).Scan(&balance)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return balance, nil
}

func (r *PostgresRepository) InsertLedgerEntry(ctx context.Context, entry LedgerEntry) error {
	query := `INSERT INTO ledger_entries (id, wallet_id, amount, type, reference_id, idempotency_key, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`

	if tx, ok := TxtContext(ctx); ok {
		_, err := tx.Exec(ctx, query,
			entry.ID,
			entry.WalletID,
			entry.Amount,
			entry.Type,
			entry.ReferenceID,
			entry.IdempotencyKey,
			entry.CreatedAt,
		)

		if err != nil {
			return err
		}

		return nil

	}

	_, err := r.pool.Exec(ctx, query,
		entry.ID,
		entry.WalletID,
		entry.Amount,
		entry.Type,
		entry.ReferenceID,
		entry.IdempotencyKey,
		entry.CreatedAt,
	)

	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresRepository) FindByIdempotencyKey(ctx context.Context, key string) (*LedgerEntry, error) {
	ledger_entry := new(LedgerEntry)

	query := `SELECT id, wallet_id, amount, type, reference_id, idempotency_key, created_at FROM ledger_entries WHERE idempotency_key = $1`

	if tx, ok := TxtContext(ctx); ok {
		err := tx.QueryRow(ctx, query, key).Scan(&ledger_entry.ID, &ledger_entry.WalletID, &ledger_entry.Amount, &ledger_entry.Type, &ledger_entry.ReferenceID, &ledger_entry.IdempotencyKey, &ledger_entry.CreatedAt)

		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		if err != nil {
			return nil, err
		}

		return ledger_entry, nil

	}
	err := r.pool.QueryRow(ctx, query, key).Scan(&ledger_entry.ID, &ledger_entry.WalletID, &ledger_entry.Amount, &ledger_entry.Type, &ledger_entry.ReferenceID, &ledger_entry.IdempotencyKey, &ledger_entry.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return ledger_entry, nil
}


func (r *PostgresRepository) LockWallet(ctx context.Context, walletID uuid.UUID) error {
	if tx, ok := TxtContext(ctx); ok {
		_, err := tx.Exec(ctx, `SELECT id FROM wallets WHERE id = $1 FOR UPDATE`, walletID)

		return err
	}

	return errors.New("lock wallet requires an active transaction")
}