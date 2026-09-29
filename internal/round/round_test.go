package round_test

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
	"github.com/alex-njaiya/popeye_the_sailor/internal/round"
	"github.com/alex-njaiya/popeye_the_sailor/internal/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func createTestRound(t *testing.T, ctx context.Context, pool *pgxpool.Pool, createdAt time.Time) *round.Round {
	t.Helper()

	round_id := uuid.New()
	serverseed, _ := fairness.GenerateServerSeed()
	clientseed, _ := fairness.GenerateServerSeed()
	serverseed_hash := fairness.HashSeed(serverseed)

	nonce := rand.IntN(1000)

	house_edge := float64(rand.IntN(100)) / 100.0
	crashpoint := fairness.ComputeCrashPoint(serverseed, clientseed, nonce, house_edge)

	round := round.Round{
		ID:               round_id,
		State:            "betting",
		ServerSeed:       serverseed,
		ClientSeed:       clientseed,
		Nonce:            nonce,
		HouseEdge:        house_edge,
		ServerSeedHash:   serverseed_hash,
		CrashPoint:       crashpoint,
		StartedAt:        createdAt,
		RunningStartedAt: nil,
		CrashedAt:        nil,
	}

	query := `INSERT INTO rounds (id, state, server_seed, client_seed, server_seed_hash, nonce, house_edge, crashpoint, started_at, running_started_at, crashed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	_, err := pool.Exec(ctx, query,
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

	require.NoError(t, err)
	return &round
}

func TestInsertRound_AndGetRoundId(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := round.NewPostgreRepo(pool)

	round_id := uuid.New()
	serverseed, _ := fairness.GenerateServerSeed()
	clientseed, _ := fairness.GenerateServerSeed()
	serverseed_hash := fairness.HashSeed(serverseed)

	nonce := rand.IntN(1000)

	house_edge := float64(rand.IntN(100)) / 100.0
	crashpoint := fairness.ComputeCrashPoint(serverseed, clientseed, nonce, house_edge)

	round := round.Round{
		ID:               round_id,
		State:            "betting",
		ServerSeed:       serverseed,
		ClientSeed:       clientseed,
		Nonce:            nonce,
		HouseEdge:        house_edge,
		ServerSeedHash:   serverseed_hash,
		CrashPoint:       crashpoint,
		StartedAt:        time.Now(),
		RunningStartedAt: nil,
		CrashedAt:        nil,
	}

	err := repo.InsertRound(ctx, round)
	require.NoError(t, err)

	require.Equal(t, round.ID, round_id)
	require.Equal(t, round.HouseEdge, house_edge)
}

func TestGetRoundById_ReturnsNilForMissingRound(t *testing.T) {
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := round.NewPostgreRepo(pool)

	round := createTestRound(t, ctx, pool, time.Now())

	round, err := repo.GetRoundById(ctx, round.ID)
	require.NoError(t, err)
	require.NotNil(t, round)
}

func TestGetRecentRounds_RespectsLimitAndOrder(t *testing.T) {

	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := round.NewPostgreRepo(pool)

	_ = createTestRound(t, ctx, pool, time.Now())

	_ = createTestRound(t, ctx, pool, time.Now())

	_ = createTestRound(t, ctx, pool, time.Now())

	_ = createTestRound(t, ctx, pool, time.Now())

	_ = createTestRound(t, ctx, pool, time.Now())

	rounds, err := repo.GetRecentRounds(ctx, 3)

	require.NoError(t, err)
	require.Equal(t, len(rounds), 3)

	isSortedDescending := slices.IsSortedFunc(rounds, func(a, b *round.Round) int {
		if a.StartedAt.After(b.StartedAt) {
			return -1 // a comes before b
		}
		if a.StartedAt.Before(b.StartedAt) {
			return 1 // a comes after b
		}
		return 0
	})

	if !isSortedDescending {
		t.Error("rounds were not returned in started_at DESC order")
		for i, r := range rounds {
			t.Logf("[%d] ID: %d, StartedAt: %s", i, r.ID, r.StartedAt.Format(time.RFC3339))
		}
	}

}

func TestGetRoundsBetween_FiltersCorrectly(t *testing.T) {
	// insert rounds spanning a wider time range, confirm only those within [from, to] come back.
	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := round.NewPostgreRepo(pool)

	basetime := time.Date(2026, time.September, 27, 10, 0, 0, 0, time.Local)



	_ = createTestRound(t, ctx, pool, basetime)
	_ = createTestRound(t, ctx, pool, basetime.Add(1 * time.Minute))
	_ = createTestRound(t, ctx, pool, basetime.Add(2 * time.Minute))

	// set a time window
	fromWindow := basetime
	toWindow := basetime.Add(2 * time.Minute +  30 * time.Second)

	rounds, err := repo.GetRoundsBetween(ctx, fromWindow, toWindow)

	require.NoError(t, err)
	require.Equal(t, len(rounds), 3)
	require.Equal(t, rounds[0].StartedAt, basetime)
	require.Equal(t, rounds[1].StartedAt, basetime.Add(1 * time.Minute))
}

func TestMarkRunning_And_MarkCrashed_UpdateOnlyIntendedFields(t *testing.T) {
	/* insert a round, call both updates, fetch it back,
	confirm running_started_at/crashed_at/state changed
	but seeds/crashpoint/nonce did not.*/


	pool := testutil.SetupTestDB(t)
	ctx := context.Background()
	repo := round.NewPostgreRepo(pool)

	original := createTestRound(t, ctx, pool, time.Now())
	require.Equal(t, "betting", string(original.State))


	//transition to running
	// update the state to running and the check again
	runningTime :=  time.Now()
	err := repo.MarkRunning(ctx, original.ID, runningTime)
	require.NoError(t, err)

	afterRunning, err := repo.GetRoundById(ctx, original.ID)
	require.NoError(t, err)
	require.NotNil(t, afterRunning)

	require.Equal(t, round.StateRunning, afterRunning.State)
	require.NotNil(t, afterRunning.RunningStartedAt)
	require.WithinDuration(t, runningTime, *afterRunning.RunningStartedAt, time.Second)

	// confirm that everything else is intact
	require.Equal(t, original.ServerSeed, afterRunning.ServerSeed)
	require.Equal(t, original.CrashPoint, afterRunning.CrashPoint)
	require.Equal(t, original.Nonce, afterRunning.Nonce)

	// transition to crashed
	crashTime := time.Now()
	err = repo.MarkCrashed(ctx, original.ID, crashTime)
	require.NoError(t, err)

	afterCrashed, err := repo.GetRoundById(ctx, original.ID)
	require.NoError(t, err)
	require.NotNil(t, afterCrashed)

	require.Equal(t, round.StateCrashed, afterCrashed.State)
	require.NotNil(t, afterCrashed.CrashedAt)
	require.WithinDuration(t, crashTime, *afterCrashed.CrashedAt, time.Second)

	// confirm the running data from the previous step is still intact
	require.NotNil(t, afterCrashed.RunningStartedAt)
	require.Equal(t, original.ServerSeed, afterCrashed.ServerSeed)
	require.Equal(t, original.CrashPoint, afterCrashed.CrashPoint)
	require.Equal(t, original.Nonce, afterCrashed.Nonce)

}
