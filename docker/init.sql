-- Initial database setup for PulseDB
-- This runs automatically when the PostgreSQL container starts for the first time

-- Grant replication privileges to our user so Debezium can connect and read the WAL
ALTER USER pulsedb_user REPLICATION;

-- Sample tables that PulseDB will track changes on
-- These represent a typical e-commerce scenario

CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(50)  NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT       NOT NULL REFERENCES users(id),
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    price       DECIMAL(10,2) NOT NULL,
    stock       INT          NOT NULL DEFAULT 0,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Audit log table — PulseDB writes every change event here for traceability
CREATE TABLE IF NOT EXISTS pulse_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    source_table    VARCHAR(100) NOT NULL,
    operation       VARCHAR(20)  NOT NULL,  -- INSERT, UPDATE, DELETE
    row_id          VARCHAR(100) NOT NULL,
    payload         JSONB        NOT NULL,
    captured_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed some sample data so we can see changes immediately on startup
INSERT INTO users (email, name, status) VALUES
    ('alice@example.com', 'Alice Johnson', 'ACTIVE'),
    ('bob@example.com',   'Bob Smith',     'ACTIVE');

INSERT INTO products (name, price, stock) VALUES
    ('Wireless Headphones', 2999.00, 50),
    ('Mechanical Keyboard', 4999.00, 30);
