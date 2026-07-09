-- ==========================================
-- DIMENSION INDEXES (For ETL Lookups)
-- ==========================================
CREATE INDEX IF NOT EXISTS idx_dim_user_bk ON gold.dim_user(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_movie_bk ON gold.dim_movie(movie_id);

-- Filtered index to quickly find the active SCD2 rows
CREATE INDEX IF NOT EXISTS idx_dim_user_current ON gold.dim_user(user_id) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_dim_movie_current ON gold.dim_movie(movie_id) WHERE is_current = TRUE;


-- ==========================================
-- FACT INDEXES (For Analytical Joins)
-- ==========================================

-- Fact Playback
CREATE INDEX IF NOT EXISTS idx_fact_pb_user ON gold.fact_playback(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_pb_movie ON gold.fact_playback(movie_key);
CREATE INDEX IF NOT EXISTS idx_fact_pb_date ON gold.fact_playback(playback_date_key);

-- Fact Rating
CREATE INDEX IF NOT EXISTS idx_fact_rt_user ON gold.fact_rating(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_rt_movie ON gold.fact_rating(movie_key);

-- Fact Payment Attempt
CREATE INDEX IF NOT EXISTS idx_fact_pa_user ON gold.fact_payment_attempt(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_pa_date ON gold.fact_payment_attempt(payment_date_key);

-- Indexing for downstream ML feature extraction lookups
CREATE INDEX IF NOT EXISTS idx_obt_playback_user_date 
ON gold.obt_playback (user_id, playback_date);

-- Indexing for feature aggregation lookups
CREATE INDEX IF NOT EXISTS idx_feat_user_90d_lookup
ON gold.feat_user_90d (user_id, snapshot_date DESC);