# Movie Streaming Data Generator 

## 1. Domain Overview

This project simulates a medium-size movie streaming platform. The generator produces:

- Offline historical/reference data (Parquet)
- Streaming real-time events (JSON)

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| users | one per user | user_id, gender, age, subscription_type, signup_ts | 
| movies | one per movie | movie_id, genre, country, runtime, language, release_year, created_ts |
| playbacks | one per movie | playback_id, user_id, movie_id, click_ts, start_ts, stop_ts, duration_watched_seconds, completion_rate |
| ratings | one per rating | rating_id, user_id, movie_id, rating, rating_ts | 
| payment_attempts | one per month per user| payment_id, user_id, payment_ts, payment_method, amount, payment_status |

### 2.2 Offline Data Problems

**Compulsory:**
- **Skew**: duration_watched_seconds skew some playbacks are played 0.0 - 0.15 of runtime and some playbacks are played 0.8-1.0 of runtime, 80% movies in Drama genre.
- **High cardinality**: user_id, movie_id, playback_id are mostly unique.
- **Schema evolution**: old partitions (60% of timeline) missing country for movies table and (20% of timeline) rating_ts for ratings table.

**Optional chosen:** 
- 2% duplicate rate in playbacks (same user_id, movie_id, start_ts repeated).

**Output:** Parquet partitioned by playback_date, rating_date, payment_date.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka/streaming topic with `event_type` field.

Key columns:
- `event_id`, `event_type` (impress|view|click|start|heartbeat|pause|resume|fast-forward|stop|complete)
- `event_ts`, `created_ts` (event time vs row creation time)
- `user_id`, `session_id`
- `movie_id` (nullable), `playback_id` (nullable), `playback_start_ts` (nullable), `duration_watch_seconds` (nullable)

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursts**: 100 events/min baseline → 3000 events/min in 20-min windows at 12:00 and 20:00.
- **Late arrivals**: 12% of events have a later `created_ts` than `event_ts`.

**Optional chosen:** 1.5% duplicate events (same event_id, immediate or 1-3 minute delay).

**Output:** JSON or Avro.

---

## 4. Feature Engineering

Compute from user historical records and clickstream event data:

**Offline (stable, 90-day windows):**
- `f_user_total_playbacks_90d` - playbacks count
- `f_user_avg_duration_watched_seconds_90d` - average watch duration value
- `f_user_distinct_genre_90d` - genre diversity
- `f_user_payment_fail_rate_90d` - payment failure ratio
- `f_user_preferred_watch_window` - preferred watch window
- `f_user_avg_completion_rate_90d` - avg completion rate

**Streaming (rolling windows):**
- `f_stream_views_30m` - count views movie details
- `f_stream_clicks_30m` - count clicks to start the movie
- `f_stream_views_to_clicks_conversion_rate_30m` - calculate views to clicks ratio
- `f_stream_distinct_movie_starts_30m` - count distinct movies' starts to detect binge watch activities
- `f_stream_burst_activity_flag` - burst period traffic

Merge offline + streaming for unified feature table keyed by user_id, refreshed every 15 minutes.

---

## 5. Generator Configuration

```yaml
n_users: 120000
n_movies: 45000
days_history: 180
skew_genre: "Drama"
skew_ratio_genre: 0.85
skew_ratio_watch_duration_seconds: 0.80
duplicate_rate_offline: 0.05
schema_change_date: "2026-02-01"
base_events_per_min: 100
burst_multiplier: 30
burst_windows: ["12:00-12:20", "20:00-20:20"]
late_arrival_rate: 0.12
late_delay_min_max: [5, 45]
duplicate_rate_stream: 0.015
random_seed: 42
```
---

## 6. Delivery and Data Quality Report

### Data Configuration Table

| Dimension | Metric / Column | Target Config | Notes |
| :--- | :--- | :--- | :--- |
| **Skew 1** | `movies.genre` | 60.0% Drama | N/A |
| **Skew 2**  | `playbacks.duration_watched_seconds` | 80.0% Bimodal (0-.15 or .8-1.0) | While 40% of movies are rarely watched (only 15% runtime), another 40% are blockbusters watched in full. The remaining 20% fall in between, averaging 15% to 80% watch time. |
| **Cardinality** | `user_id` / `movie_id` / `playback_id` | Unique / High | N/A |
| **Schema Evolution** | `movies.country` missing (50% timeline) | Missing old partitions | N/A |
| **Duplicates** | `playbacks` | 1.5% duplicates | N/A |
| **Streaming Bursts** | `events` | 100 baseline and 3000 burst | N/A |
| **Late Arrivals** | Late `created_ts` vs `event_ts` | 10.0% rate | N/A |


### Deduplication Audit Table

| Dataset | Total Rows (Before Dedup) | Calculated Duplicate Rate | Rows Remaining (After Dedup) | Expected Final Rows |
| :--- | :--- | :--- | :--- | :--- |
| **Offline Playbacks** | 105,000 | 5.0% | 100,000 | 100,000 |
| **Streaming Events** | 263,900 | 1.5% | 260,000 | 260,000 | 

### Distinct Count Audit Table
| Table Name | Target Column | Approx Count Distinct (HLL) | Actual Unique ID Volume | Cardinality Status |
| :--- | :--- | :--- | :--- | :--- |
| `playbacks` | `user_id` | ~100,000 | 100,000 | Max Cardinality (100%) |
| `playbacks` | `movie_id` | ~100,000 | 100,000 | Max Cardinality (100%) |
| `playbacks` | `start_ts` | ~100,000 | 100,000 | Max Cardinality (100%) |

## Write up

### Optional design choice

For offline playbacks data generation, 2% duplicate rate in playbacks (same user_id, movie_id, start_ts repeated). For streaming event generation, 1.5% duplicate events (same event_id within 1-3 minute delay).

### Feature engineering design

The feature engineering design is created to support the movie recommendation system for the user, which will predict which movies will have the highest expected completion rates for the users. There will be two stages, retrieval stage and ranking stage. 