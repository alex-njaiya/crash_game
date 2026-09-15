package fairness

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)


func ComputeCrashPoint(serverSeed, clientSeed string, nonce int, houseEdge float64) float64 {
	h := hmac.New(sha256.New, []byte(serverSeed))
	h.Write([]byte(fmt.Sprintf("%s-%d", clientSeed, nonce)))

	hash := h.Sum(nil)

	n := binary.BigEndian.Uint64(hash[:8])
	r := float64(n) / float64(math.MaxUint64)

	if r  == 0 {
		r = 1e-9
	}

	crash := (1 - houseEdge) / r
	return math.Max(crash, 1.0)
}