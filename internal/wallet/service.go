package wallet

import (
	"context"
	"time"

	"github.com/google/uuid"
)

var validDebitTypes = map[EntryType]bool{
	EntryWithdraw: true,
	EntryBetStake: true,
}

var validCreditTypes = map[EntryType]bool{
	EntryDeposit: true,
	EntryRefund:  true,
	EntryPayout:  true,
}

type Service struct {
	db   Transactor
	repo Repository
}

func NewService(repo Repository, db Transactor) *Service {
	return &Service{
		db, repo,
	}
}

func (s *Service) Debit(ctx context.Context, walletID uuid.UUID, amount int64, entryType EntryType, referenceID uuid.UUID, idempotencyKey string) error {
	if !validDebitTypes[entryType] {
		return ErrInvalidEntryType
	}

	if amount <= 0 {
		return ErrInvalidAmount
	}

	tx, err := s.db.Begin(ctx)

	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	txCtx := WithTx(ctx, tx)

	// find by idempotency
	existing, err := s.repo.FindByIdempotencyKey(txCtx, idempotencyKey)

	if err != nil {
		return err
	}

	if existing != nil {
		return nil
	}

	// lock the wallet and get the balance
	if err := s.repo.LockWallet(txCtx, walletID); err != nil {
		return err
	}

	balance, err := s.repo.GetBalance(txCtx, walletID)

	if err != nil {
		return err
	}

	if balance < amount {
		return ErrInsufficientBalance
	}

	entry := LedgerEntry{
		ID:             uuid.New(),
		WalletID:       walletID,
		Amount:         -amount,
		Type:           string(entryType),
		ReferenceID:    referenceID,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Now(),
	}

	// insert a ledger entry into the db
	if err := s.repo.InsertLedgerEntry(txCtx, entry); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Service) Credit(ctx context.Context, walletID uuid.UUID, amount int64, entryType EntryType, referenceId uuid.UUID, idempotencyKey string) error {
	if !validCreditTypes[entryType] {
		return ErrInvalidEntryType
	}

	if amount <= 0 {
		return ErrInvalidAmount
	}

	tx, err := s.db.Begin(ctx)

	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	txCtx := WithTx(ctx, tx)

	existing, err := s.repo.FindByIdempotencyKey(txCtx, idempotencyKey)

	if err != nil {
		return err
	}

	if existing != nil {
		return nil
	}

	// create a new entry and insert the ledger entry into the ledger entries table
	entry := LedgerEntry{
		ID:             uuid.New(),
		WalletID:       walletID,
		Amount:         amount,
		Type:           string(entryType),
		ReferenceID:    referenceId,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Now(),
	}

	if err := s.repo.InsertLedgerEntry(txCtx, entry); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
