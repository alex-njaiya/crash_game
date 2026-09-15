package simulate_test

import (
	"math"
	"testing"

	"github.com/alex-njaiya/popeye_the_sailor/cmd/simulate"
	"github.com/stretchr/testify/require"
)

func TestSampleBetSize_MeanConvergesToExpected(t *testing.T) {
	meanLog := math.Log(100)
	stdDevLog := 0.8

	var total int64
	iterations := 100000
	for i := 0; i < iterations; i++ {
		total += simulate.SampleBetSize(meanLog, stdDevLog)
	}

	avg := float64(total) / float64(iterations)

	// log-normal's mean isn't just exp(meanLog) — it's exp(meanLog + stdDevLog²/2)
	// due to the distribution's skew. Worth computing this properly:
	expectedMean := math.Exp(meanLog + (stdDevLog*stdDevLog)/2)

	require.InDelta(t, expectedMean, avg, expectedMean*0.05) // 5% tolerance
}

func TestSampleBetSize_NeverNegative(t *testing.T) {
	for i := 0; i < 10000; i++ {
		bet := simulate.SampleBetSize(math.Log(100), 0.8)
		require.GreaterOrEqual(t, bet, int64(0))
	}
}

func TestSampleCashoutTarget_NeverBelowOne(t *testing.T) {
	for i := 0; i < 10000; i++ {
		target := simulate.SampleCashoutTarget(math.Log(1.8), 0.5)
		require.GreaterOrEqual(t, target, 1.0)
	}
}