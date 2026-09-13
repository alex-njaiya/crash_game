package wallet

import "errors"

var (
	ErrInsufficientBalance = errors.New("wallet: insufficient balance")
	ErrInvalidEntryType    = errors.New("wallet: invalid entry type for this operation")
	ErrWalletNotFound      = errors.New("wallet: wallet not found")
	ErrDuplicateRequest    = errors.New("wallet: idempotency key already used with different parameters")
	ErrInvalidAmount = errors.New("wallet: invalid amount. Amount should be greater than zero")
)
