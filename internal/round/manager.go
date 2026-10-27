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

	m.rotateEpoch()
	m.currentRound = m.newRound(ctx)
	return m
}

func (m *Manager) computeMultiplierFromElapsed(elapsed time.Duration) float64 {
	seconds := elapsed.Seconds()
	return math.Exp(m.multiplierGrowthRate * seconds)
}

func (m *Manager) newRound(ctx context.Context) *Round {
	crashpoint := fairness.ComputeCrashPoint(m.serverSeed, m.clientSeed, m.nonce, m.houseEdge)
	crashpoint = math.Min(crashpoint, m.maxMultiplier)

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

	if err := m.roundStore.InsertRound(ctx, *round); err != nil {
		slog.Error("failed to persist new round", "round_id", round.ID, "err", err)
	}

	m.nonce++

	if m.nonce >= m.roundsPerEpoch {
		m.rotateEpoch()
	}

	return round
}

func (m *Manager) rotateEpoch() {
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
		m.broadcast <- EpochRevealedEvent{ServerSeed: oldSeed, ServerSeedHash: fairness.HashSeed(oldSeed)}
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
			err := m.handleBet(req)
			req.ResultCh <- err
		case req := <-m.cashoutRequests:
			err := m.handleCashout(req)
			req.ResultCh <- err
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *Manager) handleBet(req betRequest) error {
	if m.currentRound.State != StateBetting {
		return ErrBettingClosed
	}

	// call debit to minus the amount from their wallet
	idempotencyKey := fmt.Sprintf("bet-%s-%s", m.currentRound.ID, req.UserID)

	if err := m.wallet.Debit(context.Background(), req.WalletID, req.Amount, wallet.EntryBetStake, m.currentRound.ID, idempotencyKey); err != nil {
		return err
	}
	m.currentRound.Bets[req.UserID] = req.Amount
	return nil
}

func (m *Manager) handleCashout(req cashoutRequest) error {
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

	if err := m.wallet.Credit(context.Background(), req.WalletID, payout, wallet.EntryPayout, m.currentRound.ID, idempotencyKey); err != nil {
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
			m.broadcast <- TickEvent{Multiplier: multiplier}
		}

	case StateCrashed:
		if time.Since(*m.currentRound.CrashedAt) > m.cooldownWindow {
			m.startNewRound(ctx) // generate a new server seed, reset state to betting
		}
	}
}

func (m *Manager) startRunning(ctx context.Context) {
	now := time.Now()
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = &now

	if err := m.roundStore.MarkRunning(ctx, m.currentRound.ID, now); err != nil {
		slog.Error("failed to persist running state", "round_id", m.currentRound.ID, "err", err)
	}
}

func (m *Manager) crash(ctx context.Context) {
	now := time.Now()
	m.currentRound.State = StateCrashed
	m.currentRound.CrashedAt = &now

	if err := m.roundStore.MarkCrashed(ctx, m.currentRound.ID, now); err != nil {
		slog.Error("failed to persist crashed state", "round_id", m.currentRound.ID, "err", err)
	}
	// broadcast the crash event and settle the remaining bets in bets as losses since they did not cashout
	m.broadcast <- CrashEvent{Crashpoint: m.currentRound.CrashPoint, Nonce: m.currentRound.Nonce}
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
