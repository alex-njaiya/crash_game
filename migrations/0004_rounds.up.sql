CREATE TABLE IF NOT EXISTS rounds (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    state VARCHAR(10) NOT NULL CHECK (state IN ('betting', 'running', 'crashed')),
    server_seed VARCHAR(100) NOT NULL,
    client_seed VARCHAR(100) NOT NULL,
    server_seed_hash VARCHAR(100) NOT NULL,
    nonce INT NOT NULL,
    house_edge DECIMAL(5, 4) NOT NULL,
    crashpoint DECIMAL(10, 4),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    running_started_at TIMESTAMPTZ,
    crashed_at TIMESTAMPTZ,
)

CREATE INDEX idx_rounds_started_at ON rounds (started_at DESC)
CREATE INDEX idx_rounds_server_seed_hash ON rounds (server_seed_hash)