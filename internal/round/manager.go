package round

import (
	"context"
	"fmt"
	"time"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
	"github.com/alex-njaiya/popeye_the_sailor/internal/wallet"
	"github.com/google/uuid"
)

// one goroutines owns a round entirely in its own memory
// All requests come in through channels, get processed on at a time in a single loop for {select} and results
// go back out through response channels/broadcast channels

type Manager struct {
	wallet          *wallet.Service
	currentRound    *Round
	betRequests     chan betRequest
	cashoutRequests chan cashoutRequest
	broadcast       chan<- Event //outbound to websocket hub
	houseEdge       float64

	serverSeed     string // current epoch seed
	serverSeedHash string
	clientSeed     string
	nonce          int
	roundsPerEpoch int // set to something like 100

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

const bettingWindow = 5 * time.Second
const cooldownWindow = 4 * time.Second

// new manager

func NewManager(broadcast chan<- Event, walletsvc *wallet.Service, houseEdge float64, roundsPerEpoch int) *Manager {
	m := &Manager{
		wallet:          walletsvc,
		betRequests:     make(chan betRequest, 10),
		cashoutRequests: make(chan cashoutRequest, 10),
		broadcast:       broadcast,
		houseEdge:       houseEdge,
		roundsPerEpoch:  roundsPerEpoch,
	}

	m.rotateEpoch()
	m.currentRound = m.newRound()
	return m
}

func (m *Manager) newRound() *Round {
	crashpoint := fairness.ComputeCrashPoint(m.serverSeed, m.clientSeed, m.nonce, m.houseEdge)

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

	m.nonce++

	if m.nonce >= m.roundsPerEpoch {
		m.rotateEpoch()
	}

	return round
}

func (m *Manager) rotateEpoch() {
	// generate the client and server seed
	// hash the server seed and reset nonce to 0

	serverSeed, _ := fairness.GenerateServerSeed()
	clientSeed, _ := fairness.GenerateServerSeed()

	m.serverSeed = serverSeed
	m.serverSeedHash = fairness.HashSeed(serverSeed)
	m.clientSeed = clientSeed
	m.nonce = 0

	m.broadcast <- EpochRevealedEvent{ServerSeed: serverSeed, ServerSeedHash: m.serverSeedHash}
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
			m.tick()
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

	currentMultiplier := computeMultiplierFromElapsed(time.Since(m.currentRound.RunningStartedAt))

	payout := int64(float64(bet) * currentMultiplier)

	idempotencyKey := fmt.Sprintf("cashout-%s-%s", m.currentRound.ID, req.UserID)

	if err := m.wallet.Credit(context.Background(), req.WalletID, payout, wallet.EntryPayout, m.currentRound.ID, idempotencyKey); err != nil {
		return err
	}
	delete(m.currentRound.Bets, req.UserID) //mark as settled so that the crush doesn't also count them a loss
	return nil
}

func (m *Manager) tick() {
	switch m.currentRound.State {
	case StateBetting:
		if time.Since(m.currentRound.StartedAt) >= bettingWindow {
			m.startRunning()
		}
	case StateRunning:
		multiplier := computeMultiplierFromElapsed(time.Since(m.currentRound.StartedAt))

		if multiplier >= m.currentRound.CrashPoint {
			m.crash() // reveal the seed, settles remaining bets as losses, flip states
		} else {
			m.broadcast <- TickEvent{Multiplier: multiplier}
		}

	case StateCrashed:
		if time.Since(m.currentRound.CrushedAt) > cooldownWindow {
			m.startNewRound() // generate a new server seed, reset state to betting
		}
	}
}

func (m *Manager) startRunning() {
	// generate a seed and start the ticker
	m.currentRound.State = StateRunning
	m.currentRound.RunningStartedAt = time.Now()
}

func (m *Manager) crash() {
	m.currentRound.State = StateCrashed
	m.currentRound.CrushedAt = time.Now()
	// broadcast the crash event and settle the remaining bets in bets as losses since they did not cashout
	m.broadcast <- CrashEvent{Crashpoint: m.currentRound.CrashPoint, Nonce: m.currentRound.Nonce}
}

func (m *Manager) startNewRound() {
	m.currentRound = m.newRound()
}


func (m *Manager) PlaceBet(userID, walletID uuid.UUID, amount int64) error {
	resultCh := make(chan error, 1)


	m.betRequests <- betRequest{
		UserID: userID,
		WalletID: walletID,
		Amount: amount,
		ResultCh: resultCh,
	}

	return <- resultCh
}


func (m *Manager) CashOut(userID, walletID uuid.UUID) error {
	resultCh := make(chan error, 1)

	m.cashoutRequests <- cashoutRequest{
		UserID: userID,
		WalletID: walletID,
		ResultCh: resultCh,
	}

	return <- resultCh
}