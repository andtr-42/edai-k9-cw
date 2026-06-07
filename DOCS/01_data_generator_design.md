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
| movies | one per movie | movie_id, genre, country, runtime, language, release_year |
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
duplicate_rate_offline: 0.02
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

## 6. Deliverables

1. **Generator code** with configurable parameters.
2. **Data outputs**: Parquet (offline), JSON (streaming).
3. **Quality report**:
   - Skew distribution (duration_watched_seconds (0-.15,0.8-1.0)/genre %)
   - Cardinality: approx_count_distinct by ID
   - Schema evolution: nulls in old partitions
   - Duplicate rate before/after dedup
   - Streaming burst/late/duplicate rates
4. **Write-up**: explain optional problem choice and feature design.

---

## 7. Implementation Tips

- Use deterministic seeds for reproducibility.
- Define dedup keys: user_id + movie_id + start_ts (offline), event_id + created_ts (streaming).