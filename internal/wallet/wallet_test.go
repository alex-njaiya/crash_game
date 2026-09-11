package wallet_test

import (
	"context"
	"testing"

	"github.com/alex-njaiya/popeye_the_sailor/internal/testutil"
	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)


func TestCreateWallet_And_GetBalances(t *testing.T) {
	pool := testutil.SetupTestDB(t)

	ctx := context.Background()

	// insert a user manually because the user has no repository
	var UserID uuid.UUID

	query := `INSERT INTO users (phone_number, password_hash) VALUES ($1, $2) RETURNING id`

	err := pool.QueryRow(ctx, query, "254769396362", "fakehash").Scan(&UserID)
	

	require.NoError(t, err)

	repo := wallet.NewPostgresRepository(pool)

	w, err := repo.CreateWallet(ctx, UserID)

	require.NoError(t, err)
	require.Equal(t, UserID, w.UserID)
	require.Equal(t, "KES", w.Currency)


	balance, err := repo.GetBalance(ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), balance)
}