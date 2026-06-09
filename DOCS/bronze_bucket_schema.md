
### Bronze Layer Schema

| Table | Grain | Key Columns |
|-------|-------|------------|
| raw_users | one per user | raw_id (SK), user_id (BK), gender, age, subscription_type, signup_ts, _ingested_at |
| raw_movies | one per movie | raw_id (SK), movie_id (BK), genre, country, runtime, language, release_year, _ingested_at |
| raw_playbacks | one per movie | raw_id (SK), playback_id (BK), user_id, movie_id, click_ts, start_ts, stop_ts, duration_watched_seconds, completion_rate, _ingested_at |
| raw_ratings | one per rating | raw_id (SK), rating_id (BK), user_id, movie_id, rating, rating_ts  _ingested_at | 
| raw_payment_attempts | one per month per user| raw_id (SK), payment_id, user_id, payment_ts, payment_method, amount, payment_status |

Note:
_ prefix denotes system metadata and make it easy to query and govern the data

### Silver Layer Schema

| Table | Grain | Key Columns |
|-------|-------|------------|
| stg_users | one per user | user_id , gender, age, subscription_type, signup_ts, _ingested_at |
| stg_movies | one per movie | movie_id , genre, country, runtime, language, release_year, _ingested_at |
| stg_playbacks | one per movie | playback_id, user_id, movie_id, click_ts, start_ts, stop_ts, duration_watched_seconds, completion_rate, _ingested_at |
| stg_ratings | one per rating | rating_id, user_id, movie_id, rating, rating_ts  _ingested_at | 
| stg_payment_attempts | one per month per user| payment_id, user_id, payment_ts, payment_method, amount, payment_status |
