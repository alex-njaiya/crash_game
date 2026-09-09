CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    phone_number VARCHAR(15) UNIQUE,
    password_hash VARCHAR(100) NOT NULL
);