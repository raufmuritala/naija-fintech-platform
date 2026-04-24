-- =============================================================================
-- NaijaBank Mock Schema
-- Represents a Nigerian neobank (Kuda/Moniepoint style)
-- This file is auto-run when the postgres-source container starts
-- =============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- USERS
-- Nigerian customers with realistic fields
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    user_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    phone_number    VARCHAR(15) NOT NULL,      -- Nigerian format: 080XXXXXXXX
    bvn             VARCHAR(11),               -- Bank Verification Number (masked)
    state_of_origin VARCHAR(50),               -- Lagos, Abuja, Kano, Rivers, etc.
    city            VARCHAR(100),
    kyc_level       SMALLINT DEFAULT 1,        -- 1=basic, 2=standard, 3=premium
    kyc_verified_at TIMESTAMPTZ,
    is_active       BOOLEAN DEFAULT TRUE,
    referral_code   VARCHAR(10) UNIQUE,
    referred_by     UUID REFERENCES users(user_id),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- ACCOUNTS
-- Each user can have multiple account types
-- =============================================================================
CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(user_id),
    account_number  VARCHAR(10) UNIQUE NOT NULL,  -- 10-digit NUBAN format
    account_type    VARCHAR(20) NOT NULL,          -- savings, current, wallet
    currency        VARCHAR(3) DEFAULT 'NGN',
    balance         NUMERIC(18, 2) DEFAULT 0.00,
    ledger_balance  NUMERIC(18, 2) DEFAULT 0.00,  -- includes pending
    daily_limit     NUMERIC(18, 2) DEFAULT 500000.00, -- NGN 500k default
    is_frozen       BOOLEAN DEFAULT FALSE,
    freeze_reason   VARCHAR(255),
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- TRANSACTIONS
-- Core financial events — credits and debits
-- =============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id          UUID NOT NULL REFERENCES accounts(account_id),
    transaction_type    VARCHAR(30) NOT NULL,
        -- transfer_in, transfer_out, airtime, data, bill_payment,
        -- pos_purchase, atm_withdrawal, ussd, card_payment, reversal
    amount              NUMERIC(18, 2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'NGN',
    direction           VARCHAR(6) NOT NULL CHECK (direction IN ('credit', 'debit')),
    balance_before      NUMERIC(18, 2),
    balance_after       NUMERIC(18, 2),
    status              VARCHAR(20) NOT NULL DEFAULT 'pending',
        -- pending, successful, failed, reversed
    channel             VARCHAR(20),
        -- mobile_app, ussd, web, pos, atm, api
    narration           VARCHAR(255),
    reference           VARCHAR(100) UNIQUE,
    session_id          VARCHAR(100),           -- NIP session ID
    -- Counterparty (for transfers)
    counterparty_bank   VARCHAR(100),
    counterparty_acct   VARCHAR(10),
    counterparty_name   VARCHAR(255),
    -- Fee breakdown
    fee_amount          NUMERIC(18, 2) DEFAULT 0.00,
    vat_amount          NUMERIC(18, 2) DEFAULT 0.00,
    -- Metadata
    device_id           VARCHAR(100),
    ip_address          VARCHAR(45),
    location_state      VARCHAR(50),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    value_date          DATE DEFAULT CURRENT_DATE
);

-- =============================================================================
-- CARDS
-- Debit cards linked to accounts
-- =============================================================================
CREATE TABLE IF NOT EXISTS cards (
    card_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id      UUID NOT NULL REFERENCES accounts(account_id),
    user_id         UUID NOT NULL REFERENCES users(user_id),
    card_type       VARCHAR(20) DEFAULT 'virtual',   -- virtual, physical
    card_scheme     VARCHAR(20) DEFAULT 'verve',      -- verve, mastercard, visa
    masked_pan      VARCHAR(19),                      -- e.g. 5061 **** **** 1234
    expiry_month    SMALLINT,
    expiry_year     SMALLINT,
    status          VARCHAR(20) DEFAULT 'active',     -- active, blocked, expired
    daily_limit     NUMERIC(18, 2) DEFAULT 100000.00,
    issued_at       TIMESTAMPTZ DEFAULT NOW(),
    blocked_at      TIMESTAMPTZ,
    block_reason    VARCHAR(255)
);

-- =============================================================================
-- TRANSFERS
-- Peer-to-peer and interbank transfer records
-- Linked to transactions for the debit/credit legs
-- =============================================================================
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sender_account_id   UUID NOT NULL REFERENCES accounts(account_id),
    receiver_account_id UUID REFERENCES accounts(account_id),  -- null if interbank
    debit_transaction_id  UUID REFERENCES transactions(transaction_id),
    credit_transaction_id UUID REFERENCES transactions(transaction_id),
    amount              NUMERIC(18, 2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'NGN',
    transfer_type       VARCHAR(20) NOT NULL,
        -- internal (within NaijaBank), nip (interbank), neft, rtgs
    status              VARCHAR(20) DEFAULT 'pending',
        -- pending, processing, successful, failed, reversed
    -- Interbank destination
    dest_bank_code      VARCHAR(10),          -- CBN bank codes e.g. 058 = GTB
    dest_bank_name      VARCHAR(100),
    dest_account_number VARCHAR(10),
    dest_account_name   VARCHAR(255),
    narration           VARCHAR(255),
    reference           VARCHAR(100) UNIQUE,
    initiated_at        TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    failure_reason      VARCHAR(255)
);

-- =============================================================================
-- INDEXES for common query patterns
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_transactions_account_id     ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at     ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_status         ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type           ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_accounts_user_id            ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_transfers_sender            ON transfers(sender_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_initiated_at      ON transfers(initiated_at);

-- =============================================================================
-- Trigger: auto-update updated_at on row change
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
