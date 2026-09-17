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
	ID             uuid.UUID
	State          RoundState
	ServerSeed     string
	ServerSeedHash string
	CrashPoint     float64
	StartedAt      time.Time
	CrushedAt      time.Time
	Bets           map[uuid.UUID]int64
}

// Only one thing is allowed to mutate the roundstate
