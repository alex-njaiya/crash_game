package round

import "errors"


var (
	ErrBettingClosed = errors.New("round: betting round closed")
	ErrRoundNotRunning  = errors.New("round: betting round not running")
	ErrBetNotPlaced = errors.New("round: bet not placed")
)