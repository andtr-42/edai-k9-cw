## 1. Data Profile

**Input data profile (required before design)**
- Source from 01: list input datasets/events and key columns used for Bronze/Silver/Gold modeling.
    - Input data including: movies_0.6_drama_20260304, payments, playbacks, ratings_ratings_20260304 and users.parquet. 
- Data volume: estimated rows/day, historical backfill range, and expected table sizes.
    - The data volume estimated are different for different tables such as for the historical playbacks there will be 100k records over 180 days, while for ratings and payments only 50k records over 180 days. For the streaming events, there will be 260k records in 1 day. Historical backfill range would be 180 days for historical data and 1 day for streaming data. I am not sure about expected table size. 
- Data velocity: arrival/update frequency (batch interval or streaming rate).
    - The time that it takes new event happen in the world to be collected, cleaned, processed and queryable. 
- Data characteristics: key identifiers, timestamp columns, null/duplicate patterns, schema evolution risks.
- Known data issues from 01 generation: missing fields, duplicates, late-arriving records, or outliers.

**SLA targets**
- Gold table freshness: <= 30 minutes for incremental loads.
- Feature freshness: <= 5-60 minutes depending on feature type.
- Pipeline run success rate: >= 99% scheduled-run success per week.

## 2. Dimension Tables for Gold Layers

| Dimension | Grain | Key Columns |
|-------|-------|------------|
| dim_user | one per user | user_key (SK), user_id (BK), gender, age, subscription_type, signup_ts |
| dim_movie | one per movie | movie_key (SK), movie_id (BK), genre, country, runtime, language, release_year, created_at |
| dim_date | one per date | date_key (yyyymmdd), calendar_date, day_of_week, month, year, is_weekend |
| dim_time | one per second | time_key (hhmmss), time_of_day, hour, minute, second, am_pm, day_part |
| dim_rating | one per user per movie | rating_key (SK), rating (level from 1 to 5)
| dim_payment_method | one per method | payment_method_key (SK), payment_method (name) |

**Notes:**
- Use SCD2 (valid_from_ts, valid_to_ts, is_current) if attributes change over time.
- SK = surrogate key (data warehouse-generated), BK = business key (natural identifier).

## 3. Fact Tables

### 3.1 fact_playback
**Grain:** one per playback. **Keys:** user_key, movie_key, playback_date_key, click_date_key, click_time_key, start_date_key, start_time_key, stop_date_key, stop_time_key
**Other attribute columns:** click_ts, start_ts, stop_ts (?), duration_watched_seconds, completion_rate 
**Measures:** (?)
**Note:** Handles skewness, high cardinality and duplicate playbacks. 

### 3.2 fact_rating
**Grain:** one per movie per user. **Keys:** user_key, movie_key, rating_date_key, rating_time_key, rating_key
**Other attribute columns:** rating_ts (?)
**Measures:** (?)
**Note:** Schema evolution on the rating_ts 

### 3.3 fact_payment_attempt
**Grain:** one per payment. **Keys:** user_key, payment_date_key, payment_method_key.  
**Measures:** amount, is_payment_success (0/1), is_payment_failed (0/1).

## 4. OBT Table

### 4.1 obt_playback
**Grain:** one per playback
**Purpose:** Denormalized table for BI queries.  
**Columns:** playback_id, user_id, movie_id, playback_date, age, subscription_type, genre, duration_watched_seconds, completion_rate, payment_method, amount, payment_status

## 5. Refresh & Data Quality

**Refresh SLAs:**
- Dimensions: daily (or real-time if attributes change)
- Facts: incremental append/merge every 15-30 minutes
- OBT: merge by order_id every 15-30 minutes

**Quality checks:**
- Uniqueness: playback_id, rating_id, payment_id per fact table
- Referential: facts link to dimensions
- Total match check: (?)
- Duplicate check: monitor playbacks before and after dedup
- Null check: required keys/measures should stay filled

## 6. Feature Store

Keep ML features in Gold:

Each feature row should include `event_timestamp` for point-in-time joins and `created_ts` for dedup.

**Feature tables:**
1. `feat_user_90d` (grain: user_id, event_timestamp)
   - f_user_total_playbacks_90d, f_user_avg_duration_watched_seconds_90d, f_user_distinct_genre_90d
   - f_user_payment_fail_rate_90d, f_user_preferred_watch_window, f_user_avg_completion_rate_90d
2. `feat_stream_30m` (grain: user_id, event_timestamp)
   - f_stream_views_30m, f_stream_clicks_30m, f_stream_views_to_clicks_conversion_rate_30m
   - f_stream_distinct_movie_starts_30m, f_stream_burst_activity_flag
3. `feat_user_unified` (grain: user_id, event_timestamp)
   - Join offline + streaming for training/scoring

**Point-in-time correctness:** Do not use feature data later than the label/reference timestamp.

**Dedup note:** use `created_ts` to keep the latest row when multiple rows share the same entity key and `event_timestamp`.

**Refresh:** 15-60 min (feat_90d), 1-5 min (feat_stream), 5-15 min (unified).
