package fairness_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
	"github.com/stretchr/testify/require"
)


func randomServerSeed(t *testing.T) string{
	b := make([]byte, 32)

	if _, err := rand.Read(b); err != nil {
		t.Fatalf("failed to generate random bytes: %v", err)
	}

	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}

func randomClientSeed(t *testing.T) string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("failed to generate random clientseed bytes: %v", err)
	}

	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}


func randomNonce(t *testing.T) uint64 {
	nBig, err := rand.Int(rand.Reader, big.NewInt(10000000))
	if err != nil {
		return 0
	}

	return nBig.Uint64()
}

func TestGenerateServerSeed_ProducesUniqueValues(t *testing.T) {
	t.Run("validates output structure", func(t *testing.T) {
		got, err := fairness.GenerateServerSeed()

		if err != nil {
			t.Fatalf("GenrateServerSeed() returned unexpected error: %v", err)
		}

		// verify the sha256 hex is 64 chars long

		if len(got) != 64 {
			t.Errorf("expected length 64, got %d", len(got))
		}

		// verify it is a valid hexadecimal string

		if _, err := hex.DecodeString(got); err != nil {
			t.Errorf("returned string is not a valid hex: %v", err)
		}
	})

	// uniqueness and collision test
	t.Run("ensures high entropy and no duplicate seeds", func(t *testing.T) {
		numIterations := 1000
		seenSeeds := make(map[string]bool)

		for i := 0; i < numIterations; i++ {
			seed, err := fairness.GenerateServerSeed()

			if err != nil {
				t.Fatalf("failed at iteration %d: %v", i, err)
			}

			// if the seed exists we have a collsion
			if seenSeeds[seed] {
				t.Errorf("COLLISION DETECTED: seed %s was generated twice in %d runs", seed, numIterations)
			}

			seenSeeds[seed] = true
		}
	})
}

func TestHashSeed_IsDeterministic(t *testing.T) {
	seed := "fixed-test-seed"
	require.Equal(t, fairness.HashSeed(seed), fairness.HashSeed(seed))
}

func TestHashSeed_DifferentSeedsProduceDifferentHashes(t *testing.T) {
	// two distinct seeds should (essentially always) produce distinct hashes
	seed := "hash-code-one"
	seed1 := "hash-code-two"

	require.NotEqual(t, fairness.HashSeed(seed), fairness.HashSeed(seed1))
}

func TestComputeCrashPoint_IsDeterministic(t *testing.T) {
	// same (serverSeed, clientSeed, nonce) → same crash point, every call
	// this is the property that makes "provably fair" literally provable
	got1 := fairness.ComputeCrashPoint("seedA", "clientB", 5, 0.03)
	got2 := fairness.ComputeCrashPoint("seedA", "clientB", 5, 0.03)
	require.Equal(t, got1, got2)

	t.Logf("output: %v", got1)
	t.Logf("output2: %v", got2)
}

func TestComputeCrashPoint_DifferentNonceProducesDifferentPoint(t *testing.T) {
	// same seeds, different nonce → different result (proves nonce actually matters)
	got1 := fairness.ComputeCrashPoint("seedA", "clientB", 10, 0.05)
	got2 := fairness.ComputeCrashPoint("seedA", "clientB", 100, 0.05)

	require.NotEqual(t, got1, got2)
	t.Logf("output: %v", got1)
	t.Logf("output2: %v", got2)
}

// func TestComputeCrashPoint_NeverBelowOne(t *testing.T) {
// 	// run with many random seed/nonce combinations, assert every result >= 1.0
// 	iterations := 100000

// 	houseEdges := []float64{0.0, 0.2, 0.5, 0.9, 0.7, 0.1}

// 	for _,  houseEdge := range houseEdges {
// 		t.Run("houseEdge_"+string(rune(houseEdge)), func (t *testing.T){
// 			for i := 0; i < iterations; i++ {
// 				serverSeed := randomServerSeed(t)
// 				clientSeed := randomClientSeed(t)
// 				nonce := randomNonce(t)

// 				result := fairness.ComputeCrashPoint(serverSeed, clientSeed, int(nonce), houseEdge)

// 				if result < 1.0 {
// 					t.Fatalf("critical edge case failure at iteration %d: \n" + 
// 						"result: %f (less than 1)\n" +
// 						"server seed: %s\n" +
// 						"client seed: %s\n" + 
// 						"nonce: %d\n" + 
// 						"house edge: %f", 
// 						i, result, serverSeed, clientSeed, nonce, houseEdge)
// 				}

// 				t.Logf("result: %v", result)
// 			}
// 		})
// 	}
// }

func TestComputeCrashPoint_HouseEdgeConvergesOverManyRounds(t *testing.T) {
	// the "big" test: run e.g. 100,000 rounds with random seeds,
	// simulate a player who always cashes out at, say, 2.0x,
	// compute their average return, assert it's close to (1 - houseEdge)
	// within some reasonable tolerance (this is statistical, not exact)

	iterations := 100000
	cashoutTarget := 2.0 
	houseEdges := []float64{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}

	// simulating a player that cashes out at say 2.0x

	for _, houseEdge := range houseEdges {
		var totalReturn float64
		for i := 0; i < iterations; i++ {
			serverSeed := randomServerSeed(t)
			clientSeed := randomClientSeed(t)
			nonce := randomNonce(t)


			crashpoint := fairness.ComputeCrashPoint(serverSeed, clientSeed, int(nonce), houseEdge)

			if crashpoint >= cashoutTarget {
				// cashout
				totalReturn += cashoutTarget
			}
			totalReturn += 0
		}

		averageReturn := totalReturn / float64(iterations)
		expectedReturn := 1 - houseEdge


		t.Logf("house-edge=%.2f averageReturn=%.4f expected=%.4f", houseEdge, averageReturn, expectedReturn)
		require.InDelta(t, expectedReturn, averageReturn, 0.02)
	}
}

func BenchmarkGenerateServerSeed(b *testing.B) {
	for b.Loop() {
		_, _ = fairness.GenerateServerSeed()
	}
}
