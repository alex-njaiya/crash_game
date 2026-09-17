package round

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// one goroutines owns a round entirely in its own memory
// All requests come in through channels, get processed on at a time in a single loop for {select} and results
// go back out through response channels/broadcast channels


type Manager struct {
	currentRound *Round
	betRequests chan betRequest
	cashoutRequests chan cashoutRequest
	broadcast chan <- Event  //outbound to websocket hub
}

type betRequest struct {
	UserID uuid.UUID
	Amount int64
	ResultCh chan error
}


type cashoutRequest struct {
	UserID uuid.UUID
	ResultCh chan error
}


type Event struct {
	// there are 2 types of events the tick event and the crash event
	TickEvent map[string]float64
	CrashEvent map[string]float64
}


func NewManager(round *Round) *Manager {
	return &Manager{
		currentRound: round,
	}
}


func (m *Manager) Run(ctx context.Context) {
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
		case <-Event:
			return m.tick()
		}
	}
}


func (m *Manager) handleBet(req betRequest) error {
	if m.currentRound.State != StateBetting {
		return ErrBettingClosed
	}

	// call debit to minus the amount from their wallet
	m.currentRound[req.UserID] = req.Amount
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

	currentMultiplier := computeMultiplierFromElapsed(time.Since(m.currentRound.StartedAt))

	payout := int64(float64(bet) * currentMultiplier)
	delete(m.currentRound.Bets, req.UserID) //mark as settledso that the crush doesn't also count them a loss
	// call wallet.credit to credit their wallet
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
		if time.Since(m.crushedAt) > cooldownWindow {
			m.startNewRound() // generate a new server seed, reset state to betting
		}
	}
}


func (m *Manager) startRunning() {
	// generate a seed and start the ticker
}