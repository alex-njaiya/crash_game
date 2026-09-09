package wallet

import (
	"context"
	"github.com/jackc/pgx/v5"
	uuid "github.com/jackc/pgx/pgtype/ext/satori-uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

// inject an active transaction into a context
func contextWithKey(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// extract an active transaction from a context if exists
func TxtContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)

	return tx, ok
}

type Repository interface {
	CreateWallet(ctx context.Context, UserID uuid.UUID) (*Wallet, error)
	GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error)
}


type PostgreRepository struct {
	pool *pgxpool.Pool
}


func NewPostgresRespository(pool *pgxpool.Pool) *PostgreRepository {
	return &PostgreRepository{
		pool: pool,
	}
}


func (r *PostgreRepository) CreateWallet(ctx context.Context, UserID uuid.UUID) (*Wallet, error) {
	wallet := new(Wallet)

	query := `INSERT INTO wallets (userID) VALUES ($1) RETURNING id, user_id, currency, created_at`

	// check for transaction in ctx
	if tx, ok := TxtContext(ctx); ok {
		err := tx.QueryRow(ctx, query, UserID).Scan(&wallet.ID,&wallet.UserID,&wallet.Currency,&wallet.CreatedAt)

		if err != nil {
			return nil, err
		}

		return wallet, nil
	}


	// fallback to the connection pool if no transaction
	err := r.pool.QueryRow(ctx, query, UserID).Scan(&wallet.ID,&wallet.UserID,&wallet.Currency,&wallet.CreatedAt)

	if err != nil {
		return nil, err
	}

	return wallet, nil
}