DROP SCHEMA IF EXISTS gold CASCADE;

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- Dim User (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_user (
    user_key VARCHAR(32) PRIMARY KEY,       -- MD5 Hash of user_id + valid_from_ts
    user_id VARCHAR(50) NOT NULL,           -- Business Key
    gender VARCHAR(20),
    age INT,
    subscription_type VARCHAR(50),
    signup_ts TIMESTAMP,
    valid_from_ts TIMESTAMP NOT NULL,       -- SCD2 Start
    valid_to_ts TIMESTAMP,                  -- SCD2 End
    is_current BOOLEAN DEFAULT TRUE         -- SCD2 Flag
);

-- Staging User Update (Holds incoming raw batch data for dim_user)
CREATE TABLE IF NOT EXISTS gold.stg_user_update (
    user_key VARCHAR(32),
    user_id VARCHAR(50) NOT NULL,
    gender VARCHAR(20),
    age INT,
    subscription_type VARCHAR(50),
    signup_ts TIMESTAMP,
    valid_from_ts TIMESTAMP NOT NULL,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN
);

-- Dim Movie (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_movie (
    movie_key VARCHAR(32) PRIMARY KEY,      -- MD5 Hash
    movie_id VARCHAR(50) NOT NULL,          -- Business Key
    genre VARCHAR(50),
    country VARCHAR(50),
    runtime_seconds INT,
    language VARCHAR(50),
    release_year INT,
    created_at TIMESTAMP,
    valid_from_ts TIMESTAMP NOT NULL,       -- SCD2 Start
    valid_to_ts TIMESTAMP,                  -- SCD2 End
    is_current BOOLEAN DEFAULT TRUE         -- SCD2 Flag
);

-- Staging Movie Update (Holds incoming raw batch data for dim_movie)
CREATE TABLE IF NOT EXISTS gold.stg_movie_update (
    movie_key VARCHAR(32),
    movie_id VARCHAR(50) NOT NULL,
    genre VARCHAR(50),
    country VARCHAR(50),
    runtime_seconds INT,
    language VARCHAR(50),
    release_year INT,
    created_at TIMESTAMP,
    valid_from_ts TIMESTAMP NOT NULL,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN
);

-- Dim Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY,               -- Format: YYYYMMDD
    calendar_date DATE NOT NULL,
    day_of_week INT,
    month INT,
    year INT,
    is_weekend BOOLEAN
);

-- Dim Payment Method
CREATE TABLE IF NOT EXISTS gold.dim_payment_method (
    payment_method_key VARCHAR(32) PRIMARY KEY, -- MD5 Hash
    payment_method VARCHAR(50) NOT NULL
);

-- ==========================================
-- FACT TABLES
-- ==========================================

-- Fact Playback
CREATE TABLE IF NOT EXISTS gold.fact_playback (
    playback_id VARCHAR(50) PRIMARY KEY,    -- Business Key / Deduplication key
    user_key VARCHAR(32) REFERENCES gold.dim_user(user_key),
    movie_key VARCHAR(32) REFERENCES gold.dim_movie(movie_key),
    playback_date_key INT REFERENCES gold.dim_date(date_key),
    start_ts TIMESTAMP,
    duration_watched_seconds INT,
    is_completed INT DEFAULT 0,             -- Measure: 1 for yes, 0 for no
    pause_count INT DEFAULT 0               -- Measure
);

-- Fact Rating (Transaction Grain)
CREATE TABLE IF NOT EXISTS gold.fact_rating (
    rating_id VARCHAR(50) PRIMARY KEY,
    user_key VARCHAR(32) REFERENCES gold.dim_user(user_key),
    movie_key VARCHAR(32) REFERENCES gold.dim_movie(movie_key),
    rating_date_key INT REFERENCES gold.dim_date(date_key),
    rating_ts TIMESTAMP,
    rating_score INT CHECK (rating_score >= 1 AND rating_score <= 5) -- Measure
);

-- Fact Payment Attempt
CREATE TABLE IF NOT EXISTS gold.fact_payment_attempt (
    payment_attempt_id VARCHAR(50) PRIMARY KEY,
    user_key VARCHAR(32) REFERENCES gold.dim_user(user_key),
    payment_date_key INT REFERENCES gold.dim_date(date_key),
    payment_method_key VARCHAR(32) REFERENCES gold.dim_payment_method(payment_method_key),
    payment_ts TIMESTAMP NOT NULL,                          -- 🌟 Added native timestamp
    amount DECIMAL(10, 2),                  -- Measure
    currency VARCHAR(10),                   -- e.g., 'USD', 'VND'
    is_payment_success INT DEFAULT 0,       -- Measure: 1 for success
    is_payment_failed INT DEFAULT 0         -- Measure: 1 for failure
);

-- One Big Table (OBT) Playback Performance 
CREATE TABLE IF NOT EXISTS gold.obt_playback (
    playback_id VARCHAR(50) PRIMARY KEY,
    start_ts TIMESTAMP,
    duration_watched_seconds INT,
    is_completed INT DEFAULT 0,
    user_id VARCHAR(50),
    age INT,
    gender VARCHAR(20),
    subscription_type VARCHAR(50),
    movie_id VARCHAR(50),
    genre VARCHAR(50),
    runtime_seconds INT,
    release_year INT,
    playback_date DATE,
    is_weekend BOOLEAN
);

-- Feature Table: User 90-Day Aggregates
CREATE TABLE IF NOT EXISTS gold.feat_user_90d (
    user_id VARCHAR(32) NOT NULL,
    snapshot_date DATE NOT NULL,                 -- Format: YYYYMMDD
    
    -- Features
    f_user_total_playbacks_90d INT DEFAULT 0,
    f_user_distinct_genre_90d INT DEFAULT 0,
    f_user_avg_duration_watched_seconds_90d DOUBLE PRECISION DEFAULT 0.0,
    f_user_payment_fail_rate_90d DOUBLE PRECISION DEFAULT 0.0,
    f_user_avg_completion_rate_90d DOUBLE PRECISION DEFAULT 0.0,
    
    PRIMARY KEY (user_id, snapshot_date)
);