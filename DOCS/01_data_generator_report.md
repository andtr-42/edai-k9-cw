# Data Quality Report & Write Up

## Data Quality Report

### Data Configuration Table

| Dimension | Metric / Column | Target Config | Notes |
| :--- | :--- | :--- | :--- |
| **Skew** | `movies.genre` | 60.0% Drama | N/A |
|  | `playbacks.duration_watched_seconds` | 80.0% Bimodal (0-.15 or .8-1.0) | Some movies get watched only 15% of the time and some blockbusters get watched fully |
| **Cardinality** | `user_id` / `movie_id` / `playback_id` | Unique / High | N/A |
| **Schema Evolution** | `movies.country` missing (50% timeline) | Missing old partitions | N/A |
|  | `ratings.rating_ts` missing (50% timeline) | Missing old partitions | Sentinel bug replaces explicit Nulls with ancient date to partition by `rating_ts` |
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