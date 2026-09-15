package fairness

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)


func GenerateServerSeed() (string, error) {
	b := make([]byte, 32)

	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}


func HashSeed(serverSeed string) string {
	h := sha256.Sum256([]byte(serverSeed))

	return hex.EncodeToString(h[:])
}