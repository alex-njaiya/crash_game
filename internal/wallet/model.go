package wallet

import (
	"time"

	uuid "github.com/jackc/pgx/pgtype/ext/satori-uuid"
)


type Wallet struct {
	ID uuid.UUID
	UserID uuid.UUID
	Currency string
	CreatedAt time.Time
}


type LedgerEntry struct {
	ID uuid.UUID
	WalletID uuid.UUID
	Amount int64
	Type string
	ReferenceID uuid.UUID
	IdempotencyKey string
	CreatedAt time.Time
}