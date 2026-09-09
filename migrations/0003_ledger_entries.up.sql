CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    amount BIGINT NOT NULL,        -- store in cents/lowest unit, never float
    type TEXT NOT NULL CHECK (type IN ('deposit', 'withdraw', 'bet_stake','payout', 'refund')),            -- deposit, withdrawal, bet_stake, payout, refund
    reference_id UUID NOT NULL,    -- ties to bet_id, deposit_id etc.
    idempotency_key TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_ledger_entries_wallet_id ON ledger_entries (wallet_id);

CREATE VIEW wallet_balances AS
SELECT wallet_id, SUM(amount) AS balance
FROM ledger_entries
GROUP BY wallet_id;