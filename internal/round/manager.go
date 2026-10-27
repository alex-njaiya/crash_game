package round

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
)

// one goroutines owns a round entirely in its own memory
// All requests come in through channels, get processed on at a time in a single loop for {select} and results
// go back out through response channels/broadcast channels

type WalletService interface {
	Debit(ctx context.Context, walletID uuid.UUID, amount int64, entryType wallet.EntryType, referenceID uuid.UUID, idempotencyKey string) error
	Credit(ctx context.Context, walletID uuid.UUID, amount int64, entryType wallet.EntryType, referenceID uuid.UUID, idempotencyKey string) error
}

type RoundStore interface {
	InsertRound(ctx context.Context, round Round) error
	MarkRunning(ctx context.Context, roundId uuid.UUID, runningStartedAt time.Time) error
	MarkCrashed(ctx context.Context, roundId uuid.UUID, crashedAt time.Time) error
}

type Manager struct {
	wallet          WalletService
	roundStore      RoundStore
	currentRound    *Round
	betRequests     chan betRequest
	cashoutRequests chan cashoutRequest
	broadcast       chan<- Event //outbound to websocket hub
	houseEdge       float64

	serverSeed           string // current epoch seed
	serverSeedHash       string
	clientSeed           string
	nonce                int
	roundsPerEpoch       int // set to something like 100
	bettingWindow        time.Duration
	cooldownWindow       time.Duration
	multiplierGrowthRate float64
	maxMultiplier        float64
	storeTimeout         time.Duration
	walletTimeout        time.Duration
}

type betRequest struct {
	UserID   uuid.UUID
	WalletID uuid.UUID
	Amount   int64
	ResultCh chan error
}

type cashoutRequest struct {
	UserID   uuid.UUID
	WalletID uuid.UUID
	ResultCh chan error
}

// events to be broadcasted
type Event interface {
	isEvent()
}

type TickEvent struct {
	Multiplier float64
}

func (TickEvent) isEvent() {}

type CrashEvent struct {
	Crashpoint float64
	Nonce      int
}

func (CrashEvent) isEvent() {}

type EpochRevealedEvent struct {
	ServerSeed     string
	ServerSeedHash string
}

func (EpochRevealedEvent) isEvent() {}

// new manager

func NewManager(ctx context.Context, broadcast chan<- Event, walletsvc WalletService, store RoundStore, houseEdge float64, roundsPerEpoch int) *Manager {
	m := &Manager{
		wallet:               walletsvc,
		roundStore:           store,
		betRequests:          make(chan betRequest, 10),
		cashoutRequests:      make(chan cashoutRequest, 10),
		broadcast:            broadcast,
		houseEdge:            houseEdge,
		roundsPerEpoch:       roundsPerEpoch,
		bettingWindow:        5 * time.Second,
		cooldownWindow:       5 * time.Second,
		multiplierGrowthRate: 0.06,
		maxMultiplier:        100.0,
	}

	m.rotateEpoch(ctx)
	m.currentRound = m.newRound(ctx)
	return m
}

func (m *Manager) persist(ctx context.Context, op string, roundID uuid.UUID, fn func(context.Context) error) {
	ctx, cancel := context.WithTimeout(ctx, m.storeTimeout)
	defer cancel()

	if err := fn(ctx); err != nil {
		slog.Error("round persistence failed", "op", op, "round_id", roundID, "err", err)
	}
}

func (m *Manager) computeMultiplierFromElapsed(elapsed time.Duration) float64 {
	seconds := elapsed.Seconds()
	return math.Exp(m.multiplierGrowthRate * seconds)
}

func (m *Manager) newRound(ctx context.Context) *Round {
	crashpoint := fairness.CrashPoint(m.serverSeed, m.clientSeed, m.nonce, m.houseEdge, m.maxMultiplier)

	round := &Round{
		ID:             uuid.New(),
		State:          StateBetting,
		ServerSeed:     m.serverSeed,
		ServerSeedHash: m.serverSeedHash,
		ClientSeed:     m.clientSeed,
		Nonce:          m.nonce,
		CrashPoint:     crashpoint,
		StartedAt:      time.Now(),
		Bets:           make(map[uuid.UUID]int64),
	}

	m.persist(ctx, "insert_round", round.ID, func(ctx context.Context) error {
		return m.roundStore.InsertRound(ctx, *round)
	})

	m.nonce++

	if m.nonce >= m.roundsPerEpoch {
		m.rotateEpoch(ctx)
	}

	return round
}

func (m *Manager) rotateEpoch(ctx context.Context) {
	// generate the client and server seed
	// hash the server seed and reset nonce to 0

	oldSeed := m.serverSeed
	serverSeed, _ := fairness.GenerateServerSeed()
	clientSeed, _ := fairness.GenerateServerSeed()

	m.serverSeed = serverSeed
	m.serverSeedHash = fairness.HashSeed(serverSeed)
	m.clientSeed = clientSeed
	m.nonce = 0

	if oldSeed != "" {
		m.sendCritical(ctx, EpochRevealedEvent{ServerSeed: oldSeed, ServerSeedHash: fairness.HashSeed(oldSeed)})
	}
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case req := <-m.betRequests:
			req.ResultCh <- m.handleBet(ctx, req)
		case req := <-m.cashoutRequests:
			req.ResultCh <- m.handleCashout(ctx, req)
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

const walletTimeout = 3 * time.Second

func (m *Manager) handleBet(ctx context.Context, req betRequest) error {
	if m.currentRound.State != StateBetting {
		return ErrBettingClosed
	}

	// call debit to minus the amount from their wallet
	idempotencyKey := fmt.Sprintf("bet-%s-%s", m.currentRound.ID, req.UserID)

	wctx, cancel := context.WithTimeout(ctx, walletTimeout)
	defer cancel()

	if err := m.wallet.Debit(wctx, req.WalletID, req.Amount, wallet.EntryBetStake, m.currentRound.ID, idempotencyKey); err != nil {
		return err
	}
	m.currentRound.Bets[req.UserID] = req.Amount
	return nil
}

func (m *Manager) handleCashout(ctx context.Context, req cashoutRequest) error {
	if m.currentRound.State != StateRunning {
		return ErrRoundNotRunning
	}

	bet, ok := m.currentRound.Bets[req.UserID]

	if !ok {
		return ErrBetNotPlaced
	}

	currentMultiplier := m.computeMultiplierFromElapsed(time.Since(*m.currentRound.RunningStartedAt))
	payout := int64(float64(bet) * currentMultiplier)
	idempotencyKey := fmt.Sprintf("cashout-%s-%s", m.currentRound.ID, req.UserID)

	wctx, cancel := context.WithTimeout(ctx, walletTimeout)
	defer cancel()

	if err := m.wallet.Credit(wctx, req.WalletID, payout, wallet.EntryPayout, m.currentRound.ID, idempotencyKey); err != nil {
		return err
	}
	delete(m.currentRound.Bets, req.UserID) //mark as settled so that the crush doesn't also count them a loss
	return nil
}

func (m *Manager) tick(ctx context.Context) {
	switch m.currentRound.State {
	case StateBetting:
		if time.Since(m.currentRound.StartedAt) >= m.bettingWindow {
			m.startRunning(ctx)
		}
	case StateRunning:
		multiplier := m.computeMultiplierFromElapsed(time.Since(*m.currentRound.RunningStartedAt))

		if multiplier >= m.currentRound.CrashPoint {
			m.crash(ctx) // reveal the seed, settles remaining bets as losses, flip states
		} else {
			select {
			case m.broadcast <- TickEvent{Multiplier: multiplier}:
			default:
				// consumer is behind; dropping one tick is harmless; stalling the game is not
			}
		}

	case StateCrashed:
		if time.Since(*m.currentRound.CrashedAt) > m.cooldownWindow {
			m.startNewRound(ctx) // generate a new server seed, reset state to betting
		}
	}
}

func (m *Manager) sendCritical(ctx context.Context, ev Event) {
	select {
	case m.broadcast <- ev:
	case <-ctx.Done():
	}
}

func (m *Manager) startRunning(ctx context.Context) {
	now := time.Now()
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = &now
	m.persist(ctx, "mark_running", m.currentRound.ID, func(ctx context.Context) error {
		return m.roundStore.MarkRunning(ctx, m.currentRound.ID, now)
	})
}

func (m *Manager) crash(ctx context.Context) {
	now := time.Now()
	m.currentRound.State = StateCrashed
	m.currentRound.CrashedAt = &now

	// broadcast the crash event and settle the remaining bets in bets as losses since they did not cashout
	m.sendCritical(ctx, CrashEvent{Crashpoint: m.currentRound.CrashPoint, Nonce: m.currentRound.Nonce})

	m.persist(ctx, "mark_crashed", m.currentRound.ID, func(ctx context.Context) error {
		return m.roundStore.MarkCrashed(ctx, m.currentRound.ID, now)
	})

}

func (m *Manager) startNewRound(ctx context.Context) {
	m.currentRound = m.newRound(ctx)
}

func (m *Manager) PlaceBet(userID, walletID uuid.UUID, amount int64) error {
	resultCh := make(chan error, 1)

	m.betRequests <- betRequest{
		UserID:   userID,
		WalletID: walletID,
		Amount:   amount,
		ResultCh: resultCh,
	}

	return <-resultCh
}

func (m *Manager) CashOut(userID, walletID uuid.UUID) error {
	resultCh := make(chan error, 1)

	m.cashoutRequests <- cashoutRequest{
		UserID:   userID,
		WalletID: walletID,
		ResultCh: resultCh,
	}

	return <-resultCh
}
