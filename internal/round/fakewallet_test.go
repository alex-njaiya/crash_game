package round

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fakeWallet struct {
	mu        sync.Mutex
	balances  map[uuid.UUID]int64
	debitErr  error
	creditErr error
}

func newFakeWallet() *fakeWallet {
	return &fakeWallet{
		balances: make(map[uuid.UUID]int64),
	}
}

func (f *fakeWallet) Debit(ctx context.Context, walletID uuid.UUID, amount int64, entryType wallet.EntryType, referenceID uuid.UUID, idempotencykey string) error {
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

// state transition
func TestTick_BettingTransitionsToRunning_AfterWindowElapsed(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()

	m := NewManager(broadcast, fw, 0.03, 100)

	m.bettingWindow = 10 * time.Millisecond

	require.Equal(t, StateBetting, m.currentRound.State)

	time.Sleep(15 * time.Millisecond)
	m.tick()

	require.Equal(t, StateRunning, m.currentRound.State)
	require.False(t, m.currentRound.RunningStartedAt.IsZero())
}

func TestTick_RunningTransitionsToCrashed_WhenMultiplierReachesCrashPoint(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)

	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = time.Now().Add(-10 * time.Second) // pretend 10s have already elapsed
	m.currentRound.CrashPoint = 1.01                                    // force a very low, easy-to-reach target

	m.tick()

	require.Equal(t, StateCrashed, m.currentRound.State)
}

func TestHandleBet_DebitsWalletAndRecordsBet(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)

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
	m := NewManager(broadcast, fw, 0.03, 1000)
	m.currentRound.State = StateRunning // betting already closed

	err := m.handleBet(betRequest{UserID: uuid.New(), WalletID: uuid.New(), Amount: 100})
	require.ErrorIs(t, err, ErrBettingClosed)
}

func TestHandleCashout_CreditsWalletAndRemovesBet(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 100)

	userID := uuid.New()
	walletID := uuid.New()
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = time.Now() // fresh — multiplier ~1.0x
	m.currentRound.Bets[userID] = 500

	err := m.handleCashout(cashoutRequest{UserID: userID, WalletID: walletID})
	require.NoError(t, err)

	require.Greater(t, fw.balances[walletID], int64(0)) // credited something
	_, stillPresent := m.currentRound.Bets[userID]
	require.False(t, stillPresent) // removed from active bets
}

func TestHandleCashout_RejectsWhenRoundNotRunning(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)
	// currentRound.State defaults to StateBetting

	err := m.handleCashout(cashoutRequest{UserID: uuid.New(), WalletID: uuid.New()})
	require.ErrorIs(t, err, ErrRoundNotRunning)
}

func TestHandleCashout_RejectsWhenNoBetPlaced(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)
	m.currentRound.State = StateRunning

	err := m.handleCashout(cashoutRequest{UserID: uuid.New(), WalletID: uuid.New()}) // never bet
	require.ErrorIs(t, err, ErrBetNotPlaced)
}

func TestPlaceBet_SendsRequestAndReturnsResult(t *testing.T) {
	broadcast := make(chan Event, 100)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)

	userID := uuid.New()
	walletID := uuid.New()
	fw.balances[walletID] = 1000

	// simulate what Run()'s select loop would do, without running the full loop
	go func() {
		req := <-m.betRequests
		req.ResultCh <- m.handleBet(req)
	}()

	err := m.PlaceBet(userID, walletID, 500)
	require.NoError(t, err)
	require.Equal(t, int64(500), m.currentRound.Bets[userID])
}

func TestRun_HandlesConcurrentPlaceBetsSafely(t *testing.T) {
	broadcast := make(chan Event, 1000) // generously buffered so ticks don't block the test
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)
	m.bettingWindow = 10 * time.Second // keep betting open for the whole test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			userID := uuid.New()
			walletID := uuid.New()
			fw.mu.Lock()
			fw.balances[walletID] = 1000
			fw.mu.Unlock()

			err := m.PlaceBet(userID, walletID, 100)
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// give the manager's goroutine a moment to have processed everything
	// (all sends already completed synchronously via PlaceBet's blocking receive,
	// so by the time wg.Wait() returns, every bet has actually been recorded)
	m2 := m // just for clarity — no separate sync needed, see note below
	_ = m2
	require.Len(t, m.currentRound.Bets, 20)
}

func TestRun_FullRoundLifecycle_ThroughRealTicker(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.99, 1000) // near-1.0x house edge — crashes almost immediately
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	var sawCrash bool
	timeout := time.After(3 * time.Second)

	for !sawCrash {
		select {
		case ev := <-broadcast:
			if _, ok := ev.(CrashEvent); ok {
				sawCrash = true
			}
		case <-timeout:
			t.Fatal("timed out waiting for a CrashEvent — round never crashed")
		}
	}
}

func TestRun_TransitionsThroughFullLifecycleTwice(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.99, 1000)
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	crashCount := 0
	timeout := time.After(5 * time.Second)

	for crashCount < 2 { // prove it can go around the loop more than once
		select {
		case ev := <-broadcast:
			if _, ok := ev.(CrashEvent); ok {
				crashCount++
			}
		case <-timeout:
			t.Fatalf("timed out after %d crashes, expected 2", crashCount)
		}
	}
}

func TestRun_ObserveCrashPointsAcrossRounds(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.03, 1000)
	m.bettingWindow = 50 * time.Millisecond
	m.cooldownWindow = 50 * time.Millisecond
	m.multiplierGrowthRate = 1.0
	m.maxMultiplier = 3.0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
				elapsed := time.Since(runningStartedAt)
				t.Logf("round %d: crashed at %.4fx (nonce=%d) — running phase lasted ~%v",
					seen, e.Crashpoint, e.Nonce, elapsed)
				sawFirstTickThisRound = false // reset for the next round
			case EpochRevealedEvent:
				t.Logf("epoch rotated — server seed revealed: %s", e.ServerSeed)
			}
		case <-timeout:
			t.Fatalf("timed out after observing %d/%d rounds", seen, wantRounds)
		}
	}
}

func TestNewRound_CrashPointNeverExceedsMaxMultiplier(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	m := NewManager(broadcast, fw, 0.9, 1000) // very low edge — pushes crash points high often
	m.maxMultiplier = 5.0

	for i := 0; i < 1000; i++ {
		round := m.newRound()
		require.LessOrEqual(t, round.CrashPoint, m.maxMultiplier,
			"round %d: crash point %.4f exceeded cap %.4f", i, round.CrashPoint, m.maxMultiplier)
	}
}

func TestNewRound_RotatesEpochExactlyAtBoundary(t *testing.T) {
	broadcast := make(chan Event, 1000)
	fw := newFakeWallet()
	roundsPerEpoch := 5
	m := NewManager(broadcast, fw, 0.03, roundsPerEpoch)

	initialSeed := m.serverSeed
	initialHash := m.serverSeedHash

	// NO drain here — NewManager's first rotateEpoch() no longer broadcasts

	require.Equal(t, 0, m.currentRound.Nonce)

	for nonce := 1; nonce < roundsPerEpoch; nonce++ {
		round := m.newRound()
		require.Equal(t, initialSeed, round.ServerSeed)
		require.Equal(t, nonce, round.Nonce)
		t.Logf("seed: %v", m.serverSeed)
	}

	require.NotEqual(t, initialSeed, m.serverSeed)
	require.NotEqual(t, initialHash, m.serverSeedHash)
	require.Equal(t, 0, m.nonce)

	ev := <-broadcast // this is now the FIRST broadcast that's actually happened
	revealed, ok := ev.(EpochRevealedEvent)
	require.True(t, ok)
	require.Equal(t, initialSeed, revealed.ServerSeed)
}