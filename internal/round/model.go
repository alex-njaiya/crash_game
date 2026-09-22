package round

import (
	"time"

	"github.com/google/uuid"
)

type RoundState string

const (
	StateBetting RoundState = "betting"
	StateRunning RoundState = "running"
	StateCrashed RoundState = "crashed"
)

type Round struct {
	ID               uuid.UUID
	State            RoundState
	ServerSeed       string
	ClientSeed       string
	Nonce            int
	HouseEdge        float64
	ServerSeedHash   string
	CrashPoint       float64
	StartedAt        time.Time
	RunningStartedAt time.Time
	CrashedAt        time.Time
	Bets             map[uuid.UUID]int64
}
