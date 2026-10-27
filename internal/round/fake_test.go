package round

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ---------- fakes ----------

type fakeWallet struct {
	mu        sync.Mutex
	balances  map[uuid.UUID]int64
	debitErr  error
	creditErr error
}

func newFakeWallet() *fakeWallet {
	return &fakeWallet{balances: make(map[uuid.UUID]int64)}
}

func (f *fakeWallet) Debit(ctx context.Context, walletID uuid.UUID, amount int64, entryType wallet.EntryType, referenceID uuid.UUID, idempotencyKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.debitErr != nil {
		return f.debitErr
	}
	f.balances[walletID] -= amount
	return nil
}

func (f *fakeWallet) Credit(ctx context.Context, walletID uuid.UUID, amount int64, entryType wallet.EntryType, referenceID uuid.UUID, idempotencyKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.creditErr != nil {
		return f.creditErr
	}
	f.balances[walletID] += amount
	return nil
}

type fakeRoundStore struct {
	mu        sync.Mutex
	inserted  []Round
	running   []uuid.UUID
	crashed   []uuid.UUID
	insertErr error // set before NewManager to simulate a failing database
}

func newFakeRoundStore() *fakeRoundStore {
	return &fakeRoundStore{}
}

func (f *fakeRoundStore) InsertRound(ctx context.Context, r Round) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = append(f.inserted, r)
	return nil
}

func (f *fakeRoundStore) MarkRunning(ctx context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = append(f.running, id)
	return nil
}

func (f *fakeRoundStore) MarkCrashed(ctx context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashed = append(f.crashed, id)
	return nil
}

// locked accessors, safe to call while Run() is active
func (f *fakeRoundStore) insertedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inserted)
}

func (f *fakeRoundStore) runningIDs() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.running...)
}

func (f *fakeRoundStore) crashedIDs() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.crashed...)
}

// ---------- state transitions (direct calls, no Run loop) ----------

func TestTick_BettingTransitionsToRunning_AfterWindowElapsed(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 100)

	m.bettingWindow = 10 * time.Millisecond
	require.Equal(t, StateBetting, m.currentRound.State)
	require.Equal(t, 1, store.insertedCount()) // NewManager persisted the first round

	time.Sleep(15 * time.Millisecond)
	m.tick(ctx)

	require.Equal(t, StateRunning, m.currentRound.State)
	require.NotNil(t, m.currentRound.RunningStartedAt)
	require.Equal(t, []uuid.UUID{m.currentRound.ID}, store.runningIDs())
}

func TestTick_RunningTransitionsToCrashed_WhenMultiplierReachesCrashPoint(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)

	runningStart := time.Now().Add(-10 * time.Second) // pretend 10s have elapsed
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = &runningStart
	m.currentRound.CrashPoint = 1.01 // very low, easy-to-reach target

	m.tick(ctx)

	require.Equal(t, StateCrashed, m.currentRound.State)
	require.NotNil(t, m.currentRound.CrashedAt)
	require.Equal(t, []uuid.UUID{m.currentRound.ID}, store.crashedIDs())
}

func TestNewRound_ContinuesWhenInsertFails(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	store.insertErr = errors.New("db down") // set before NewManager, which creates the first round
	ctx := context.Background()

	m := NewManager(ctx, broadcast, fw, store, 0.03, 100)

	// persistence failure is logged, not fatal: the round still exists in memory
	require.NotNil(t, m.currentRound)
	require.Equal(t, StateBetting, m.currentRound.State)
	require.Equal(t, 0, store.insertedCount())
}

// ---------- handleBet / handleCashout (direct calls) ----------

func TestHandleBet_DebitsWalletAndRecordsBet(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)

	userID := uuid.New()
	walletID := uuid.New()
	fw.balances[walletID] = 1000

	err := m.handleBet(betRequest{UserID: userID, WalletID: walletID, Amount: 500})
	require.NoError(t, err)

	require.Equal(t, int64(500), fw.balances[walletID])       // debited
	require.Equal(t, int64(500), m.currentRound.Bets[userID]) // recorded
}

func TestHandleBet_RejectsWhenBettingClosed(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)
	m.currentRound.State = StateRunning // betting already closed

	err := m.handleBet(betRequest{UserID: uuid.New(), WalletID: uuid.New(), Amount: 100})
	require.ErrorIs(t, err, ErrBettingClosed)
}

func TestHandleCashout_CreditsWalletAndRemovesBet(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 100)

	userID := uuid.New()
	walletID := uuid.New()
	now := time.Now()
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = &now // fresh, so the multiplier is ~1.0x
	m.currentRound.Bets[userID] = 500

	err := m.handleCashout(cashoutRequest{UserID: userID, WalletID: walletID})
	require.NoError(t, err)

	require.InDelta(t, 500, fw.balances[walletID], 10) // ~1.0x payout on a 500 stake
	_, stillPresent := m.currentRound.Bets[userID]
	require.False(t, stillPresent) // removed from active bets
}

func TestHandleCashout_RejectsWhenRoundNotRunning(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)
	// round starts in StateBetting

	err := m.handleCashout(cashoutRequest{UserID: uuid.New(), WalletID: uuid.New()})
	require.ErrorIs(t, err, ErrRoundNotRunning)
}

func TestHandleCashout_RejectsWhenNoBetPlaced(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)
	m.currentRound.State = StateRunning

	err := m.handleCashout(cashoutRequest{UserID: uuid.New(), WalletID: uuid.New()}) // never bet
	require.ErrorIs(t, err, ErrBetNotPlaced)
}

// ---------- channel plumbing ----------

func TestPlaceBet_SendsRequestAndReturnsResult(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)

	userID := uuid.New()
	walletID := uuid.New()
	fw.balances[walletID] = 1000

	// stand in for what Run()'s select loop would do, without running the full loop
	go func() {
		req := <-m.betRequests
		req.ResultCh <- m.handleBet(req)
	}()

	err := m.PlaceBet(userID, walletID, 500)
	require.NoError(t, err)
	require.Equal(t, int64(500), m.currentRound.Bets[userID])
}

// ---------- tests that run the real Run() loop ----------

func TestRun_HandlesConcurrentPlaceBetsSafely(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcast := make(chan Event, 1000) // buffered so ticks never block the manager
	fw := newFakeWallet()
	store := newFakeRoundStore()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)
	m.bettingWindow = 10 * time.Second // keep betting open for the whole test

	go m.Run(ctx)

	walletIDs := make([]uuid.UUID, 20)
	for i := range walletIDs {
		walletIDs[i] = uuid.New()
		fw.mu.Lock()
		fw.balances[walletIDs[i]] = 1000
		fw.mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := m.PlaceBet(uuid.New(), walletIDs[i], 100); err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// Every PlaceBet blocks until the manager has fully processed its request,
	// so by now all 20 writes happened-before this read. No sleep needed.
	require.Len(t, m.currentRound.Bets, 20)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	for _, id := range walletIDs {
		require.Equal(t, int64(900), fw.balances[id])
	}
}

func TestRun_FullRoundLifecycle_ThroughRealTicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	m := NewManager(ctx, broadcast, fw, store, 0.99, 1000) // very high edge: crashes almost immediately
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond

	go m.Run(ctx)

	timeout := time.After(3 * time.Second)
	sawCrash := false
	for !sawCrash {
		select {
		case ev := <-broadcast:
			if _, ok := ev.(CrashEvent); ok {
				sawCrash = true
			}
		case <-timeout:
			t.Fatal("timed out waiting for a CrashEvent: round never crashed")
		}
	}

	require.NotEmpty(t, store.runningIDs())
	require.NotEmpty(t, store.crashedIDs())
}

func TestRun_TransitionsThroughFullLifecycleTwice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	m := NewManager(ctx, broadcast, fw, store, 0.99, 1000)
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond

	go m.Run(ctx)

	crashCount := 0
	timeout := time.After(5 * time.Second)
	for crashCount < 2 { // prove the cycle can repeat
		select {
		case ev := <-broadcast:
			if _, ok := ev.(CrashEvent); ok {
				crashCount++
			}
		case <-timeout:
			t.Fatalf("timed out after %d crashes, expected 2", crashCount)
		}
	}

	require.GreaterOrEqual(t, store.insertedCount(), 2) // a new round was persisted after the first crash
}

func TestRun_ObserveCrashPointsAcrossRounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	m := NewManager(ctx, broadcast, fw, store, 0.03, 1000)
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond
	m.multiplierGrowthRate = 1.0
	m.maxMultiplier = 3.0

	go m.Run(ctx)

	wantRounds := 10
	seen := 0
	timeout := time.After(15 * time.Second)

	var runningStartedAt time.Time
	sawFirstTickThisRound := false

	for seen < wantRounds {
		select {
		case ev := <-broadcast:
			switch e := ev.(type) {
			case TickEvent:
				if !sawFirstTickThisRound {
					runningStartedAt = time.Now()
					sawFirstTickThisRound = true
				}
			case CrashEvent:
				seen++
				t.Logf("round %d: crashed at %.4fx (nonce=%d), running phase lasted ~%v",
					seen, e.Crashpoint, e.Nonce, time.Since(runningStartedAt))
				sawFirstTickThisRound = false
			case EpochRevealedEvent:
				t.Logf("epoch rotated, server seed revealed: %s", e.ServerSeed)
			}
		case <-timeout:
			t.Fatalf("timed out after observing %d/%d rounds", seen, wantRounds)
		}
	}
}

// ---------- fairness / epoch behavior ----------

func TestNewRound_CrashPointNeverExceedsMaxMultiplier(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	ctx := context.Background()
	// LOW house edge makes high crash points common, so the cap actually gets exercised
	m := NewManager(ctx, broadcast, fw, store, 0.01, 1000)
	m.maxMultiplier = 5.0

	for i := 0; i < 1000; i++ {
		round := m.newRound(ctx)
		require.LessOrEqual(t, round.CrashPoint, m.maxMultiplier,
			"round %d: crash point %.4f exceeded cap %.4f", i, round.CrashPoint, m.maxMultiplier)
	}
}

func TestNewRound_RotatesEpochExactlyAtBoundary(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	store := newFakeRoundStore()
	roundsPerEpoch := 5
	ctx := context.Background()
	m := NewManager(ctx, broadcast, fw, store, 0.03, roundsPerEpoch)

	initialSeed := m.serverSeed
	initialHash := m.serverSeedHash

	// NewManager already created the epoch's first round (nonce 0)
	require.Equal(t, 0, m.currentRound.Nonce)

	for nonce := 1; nonce < roundsPerEpoch; nonce++ {
		round := m.newRound(ctx)
		require.Equal(t, initialSeed, round.ServerSeed)
		require.Equal(t, nonce, round.Nonce)
	}

	require.NotEqual(t, initialSeed, m.serverSeed)
	require.NotEqual(t, initialHash, m.serverSeedHash)
	require.Equal(t, 0, m.nonce)
	require.Equal(t, roundsPerEpoch, store.insertedCount()) // one persisted round per nonce

	ev := <-broadcast // the first broadcast that has actually happened
	revealed, ok := ev.(EpochRevealedEvent)
	require.True(t, ok)
	require.Equal(t, initialSeed, revealed.ServerSeed) // the OLD seed gets revealed
}