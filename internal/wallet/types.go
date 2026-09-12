package wallet

type EntryType string

const (
	EntryDeposit  EntryType = "deposit"
	EntryWithdraw EntryType = "withdraw"
	EntryBetStake EntryType = "bet_stake"
	EntryPayout   EntryType = "payout"
	EntryRefund   EntryType = "refund"
)
