package wallet_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/testutil"
	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func createTestWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *wallet.Wallet {
	t.Helper()

	// insert a user manually because the user has no repository
	var UserID uuid.UUID

	query := `INSERT INTO users (phone_number, password_hash) VALUES ($1, $2) RETURNING id`

	err := pool.QueryRow(ctx, query, fmt.Sprintf("2547%08d", rand.Intn(99999999)), "fakehash").Scan(&UserID)

	require.NoError(t, err)

	repo := wallet.NewPostgresRepository(pool)

	w, err := repo.CreateWallet(ctx, UserID)

	require.NoError(t, err)
	require.Equal(t, UserID, w.UserID)
	require.Equal(t, "KES", w.Currency)

	return w
}

func TestCreateWallet_And_GetBalances(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := wallet.NewPostgresRepository(pool)

	// insert a user manually because the user has no repository
	w := createTestWallet(t, ctx, pool)

	balance, err := repo.GetBalance(ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), balance)
}

func TestInsertLedgerEntry_And_FindByIdempotencyKey(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := wallet.NewPostgresRepository(pool)

	w := createTestWallet(t, ctx, pool)

	entry := wallet.LedgerEntry{
		ID:             uuid.New(),
		WalletID:       w.ID,
		Amount:         1000,
		Type:           "deposit",
		ReferenceID:    uuid.New(),
		IdempotencyKey: "test-key-123",
		CreatedAt:      time.Now(),
	}

	entry1 := wallet.LedgerEntry{
		ID:             uuid.New(),
		WalletID:       w.ID,
		Amount:         -300,
		Type:           "bet_stake",
		ReferenceID:    uuid.New(),
		IdempotencyKey: "test-key-456",
		CreatedAt:      time.Now(),
	}

	err := repo.InsertLedgerEntry(ctx, entry)
	require.NoError(t, err)

	balance, err := repo.GetBalance(ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1000), balance)

	err = repo.InsertLedgerEntry(ctx, entry1)
	require.NoError(t, err)

	balance, err = repo.GetBalance(ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, int64(700), balance)

	found, err := repo.FindByIdempotencyKey(ctx, "test-key-123")
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, entry.Amount, found.Amount)

	notFound, err := repo.FindByIdempotencyKey(ctx, "nonexistent-key")
	require.NoError(t, err)
	require.Nil(t, notFound)
}

func TestConcurrentDebit_WithoutLocking_AllowsOverdraft(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := wallet.NewPostgresRepository(pool)

	// create a user + wallet and credit to the wallet
	w := createTestWallet(t, ctx, pool)

	entry := wallet.LedgerEntry{
		ID:             uuid.New(),
		WalletID:       w.ID,
		Amount:         1000,
		Type:           "deposit",
		ReferenceID:    uuid.New(),
		IdempotencyKey: "test-new-key-123",
		CreatedAt:      time.Now(),
	}

	err := repo.InsertLedgerEntry(ctx, entry)
	require.NoError(t, err)

	// fire 2 goroutines that attempt to debit 800 each without a lock
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			balance, _ := repo.GetBalance(ctx, w.ID)

			time.Sleep(50 * time.Millisecond)

			if balance >= 800 {
				err := repo.InsertLedgerEntry(ctx, wallet.LedgerEntry{
					ID:             uuid.New(),
					WalletID:       w.ID,
					Amount:         -800,
					Type:           "withdraw",
					ReferenceID:    uuid.New(),
					IdempotencyKey: fmt.Sprintf("test-debit-%d", i),
					CreatedAt:      time.Now(),
				})

				if err != nil {
					t.Logf("goroutine %d insert failed: %v", i, err)
				}
			}
		}(i)
	}
	wg.Wait()

	finalBalance, _ := repo.GetBalance(ctx, w.ID)
	t.Logf("final balance: %d", finalBalance)
}

func TestConcurrentDebit_WithLocking_NoOverdraft(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := wallet.NewPostgresRepository(pool)

	// create a user + wallet and debit 1000
	w := createTestWallet(t, ctx, pool)

	entry := wallet.LedgerEntry{
		ID:             uuid.New(),
		WalletID:       w.ID,
		Amount:         1000,
		Type:           "deposit",
		ReferenceID:    uuid.New(),
		IdempotencyKey: "new-key-123",
		CreatedAt:      time.Now(),
	}

	err := repo.InsertLedgerEntry(ctx, entry)
	require.NoError(t, err)

	// fire goroutines to test debiting with wallet locking
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			tx, err := pool.Begin(ctx)

			if err != nil {
				t.Logf("goroutine %d begin failed: %v", i, err)
			}

			defer tx.Rollback(ctx)

			txCtx := wallet.WithTx(ctx, tx)

			// lock the wallet first, this is what makes the goroutines queue up
			if err := repo.LockWallet(txCtx, w.ID); err != nil {
				t.Logf("goroutine %d lock failed: %v", i, err)
				return
			}

			time.Sleep(50 * time.Millisecond)

			balance, err := repo.GetBalance(txCtx, w.ID)

			if err != nil {
				t.Logf("goroutine %d balance check failed: %v", i, err)
				return
			}

			if balance >= 200 {
				new_entry := wallet.LedgerEntry{
					ID:             uuid.New(),
					WalletID:       w.ID,
					Amount:         -200,
					Type:           "bet_stake",
					ReferenceID:    uuid.New(),
					IdempotencyKey: fmt.Sprintf("test-debit-%d", i),
					CreatedAt:      time.Now(),
				}
				if err := repo.InsertLedgerEntry(txCtx, new_entry); err != nil {
					t.Logf("goroutine %d insert failed: %v", i, err)
					return
				}
			}

			if err := tx.Commit(ctx); err != nil{
				t.Logf("goroutine %d commit failed: %v", i, err)
			}

		}(i)
	}
	wg.Wait()

	finalBalance, _ := repo.GetBalance(ctx, w.ID)
	t.Logf("final balance: %d", finalBalance)

}
