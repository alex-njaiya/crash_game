package simulate

import (
	"math"
	"math/rand"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
)

type SimConfig struct {
	HouseEdge        float64
	StartingBankroll int64
	NumRounds        int
	AvgBetSize       float64
	BetSizeStdDev    float64
	AvgCashoutTarget float64
	CashoutStdDev    float64
	PlayersPerRound  int
}

type SimResult struct {
	FinalBankroll   int64
	MinBankroll     int64   // lowest point ever reached
	MaxDrawdown     int64   // StartingBankroll - MinBankroll
	BankrollHistory []int64 // for plotting/inspection
}

func SampleBetSize(meanLog, stdDevLog float64) int64 {
	// rand.NormFloat64() draws from a standard normal distribution (mean 0, stddev 1)
	normalSample := rand.NormFloat64()

	// scale and shift it, then exponentiate — this is the definition of log-normal
	logNormalSample := math.Exp(meanLog + stdDevLog*normalSample)

	return int64(logNormalSample)
}

// avgBet := 10.0
// betSize := sampleBetSize(math.Log(avgBet), 0.8) // 0.8 is a reasonable starting spread — tune by testing

func SampleCashoutTarget(meanLog, stdDevLog float64) float64 {
	normalSample := rand.NormFloat64()
	target := math.Exp(meanLog + stdDevLog*normalSample)

	if target < 1.0 {
		target = 1.0 // floor — matches the same floor your crash point has
	}
	return target
}

// avgTarget := 1.8
// target := sampleCashoutTarget(math.Log(avgTarget), 0.5)

func RunSimulation(cfg SimConfig) SimResult {
	bankroll := cfg.StartingBankroll
	minBankroll := bankroll
	history := make([]int64, 0, cfg.NumRounds)

	for round := 0; round < cfg.NumRounds; round++ {
		serverSeed, _ := fairness.GenerateServerSeed()
		clientSeed := "sim-client" // fine to hardcode for pure simulation
		crashPoint := fairness.ComputeCrashPoint(serverSeed, clientSeed, round, cfg.HouseEdge)

		var roundstakes, roundPayouts int64
		for p := 0; p < cfg.PlayersPerRound; p++ {
			betSize := SampleBetSize(math.Log(cfg.AvgBetSize), cfg.BetSizeStdDev)
			cashoutTarget := SampleCashoutTarget(math.Log(cfg.AvgCashoutTarget), cfg.CashoutStdDev)

			roundstakes += betSize

			if crashPoint >= cashoutTarget {
				roundPayouts += int64(float64(betSize) * cashoutTarget)
			}

			bankroll += roundstakes - roundPayouts
			history = append(history, bankroll)

			if bankroll < minBankroll {
				minBankroll = bankroll
			}
		}
	}

	return SimResult{
		FinalBankroll:   bankroll,
		MinBankroll:     minBankroll,
		MaxDrawdown:     cfg.StartingBankroll - minBankroll,
		BankrollHistory: history,
	}
}

// type MultiSessionResult struct {
// 	NumSessions    int
// 	Drawdowns      []int64 // one MaxDrawdown per session, sorted ascending
// 	MedianDrawdown int64
// 	P95Drawdown    int64
// 	P99Drawdown    int64
// 	WorstDrawdown  int64
// 	FinalBankrolls []int64 // useful to inspect alongside drawdowns
// }

// func RunMultiSession(cfg SimConfig, numSessions int) MultiSessionResult {
// 	drawdowns := make([]int64, numSessions)
// 	finalBankrolls := make([]int64, numSessions)

// 	for i := 0; i < numSessions; i++ {
// 		result := RunSimulation(cfg)
// 		drawdowns[i] = result.MaxDrawdown
// 		finalBankrolls[i] = result.FinalBankroll
// 	}

// 	sort.Slice(drawdowns, func(i, j int) bool { return drawdowns[i] < drawdowns[j] })

// 	return MultiSessionResult{
// 		NumSessions:    numSessions,
// 		Drawdowns:      drawdowns,
// 		FinalBankrolls: finalBankrolls,
// 		MedianDrawdown: percentile(drawdowns, 0.50),
// 		P95Drawdown:    percentile(drawdowns, 0.95),
// 		P99Drawdown:    percentile(drawdowns, 0.99),
// 		WorstDrawdown:  drawdowns[len(drawdowns)-1],
// 	}
// }
