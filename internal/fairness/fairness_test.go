package fairness_test

import (
	"encoding/hex"
	"testing"

	"github.com/alex-njaiya/popeye_the_sailor/internal/fairness"
	"github.com/stretchr/testify/require"
)



func TestGenerateServerSeed_ProducesUniqueValues(t *testing.T) {
	t.Run("validates output structure", func(t *testing.T){
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
	t.Run("ensures high entropy and no duplicate seeds", func(t *testing.T){
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


func BenchmarkGenerateServerSeed(b *testing.B) {
	for b.Loop() {
		_, _ =  fairness.GenerateServerSeed()
	}
}
