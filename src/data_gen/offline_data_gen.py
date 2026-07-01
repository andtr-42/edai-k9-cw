"""
Module: src.generation.offline_data_gen
Type: Component (Data Generation / Seeding Pipeline)

Description:
    Generates reproducible, high-fidelity offline synthetic datasets for the
    streaming data platform. Simulates historical user behavior, movie metadata
    (including schema drift events), playback tracking, ratings, and financial
    transactions. Designed to seed a downstream 'Bronze' layer data lake.

Data Models Generated:
    1. Users (users.parquet): Unpartitioned demographic profile logs.
    2. Movies (movies/): Monthly unpartitioned shards with realistic genre distribution 
       skewing and post-schema-evolution country/language enforcement.
    3. Playbacks (playbacks/): Hive-partitioned logs (by playback_date) tracking user 
       video playback engagement with vectorized bimodal completion rates.
    4. Ratings (ratings/): Hive-partitioned logs (by rating_date) tracking content reviews.
    5. Payments (payments/): Hive-partitioned logs (by payment_date) detailing customer transactions.

Engine Features & Guardrails:
    - Microsecond Time Precision: Simulates organic back-dated transaction logs based on a anchor date.
    - Determinism: Controlled via an explicit global NumPy random seed.
    - Idempotency: Clears target directory structures locally before starting a fresh run.
    - PyArrow Integration: Performs high-efficiency dataset serialization using native Hive constructs.

Runtime Context:
    Must be executed from the project root repository directory as a Python module:
    $ python -m src.generation.offline_data_gen
"""

import os
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
from src.config import OFFLINE_DATA_PATH

# ==========================================
# 1. CONFIGURATION
# ==========================================
RANDOM_SEED = 42

USER_CONFIG = {
    "genders": ["M", "F", "O"],
    "min_age": 16,
    "max_age": 80,
    "subscription_types": ["Standard-With-Ads", "Standard", "Premium"],
}

MOVIE_CONFIG = {
    "genres": [
        "Action",
        "Comedy",
        "Drama",
        "Horror",
        "Sci-Fi",
        "Romance",
        "Documentary",
        "Family",
        "Animation",
        "Fantasy",
    ],
    "countries": [
        "USA",
        "UK",
        "France",
        "Vietnam",
        "Japan",
        "Portugal",
        "Italy",
        "South Korea",
        "China",
        "Spain",
    ],
    "languages": [
        "English",
        "French",
        "Vietnamese",
        "Japanese",
        "Portuguese",
        "Italian",
        "Korean",
        "Chinese",
        "Spanish",
    ],
    "language_by_country": {
    "USA": "English",
    "UK": "English",
    "France": "French",
    "Vietnam": "Vietnamese",
    "Japan": "Japanese",
    "Portugal": "Portuguese",
    "Italy": "Italian",
    "South Korea": "Korean",
    "China": "Chinese",
    "Spain": "Spanish"
    },
    "min_runtime_seconds": 30 * 60, # 30 mins in seconds
    "max_runtime_seconds": 3 * 60 * 60,  # 3 hours in seconds
    "start_release_year": 1970,
    "end_release_year": 2010,
}
PLAYBACK_CONFIG = {
    "bounce_rate_range": (0.01, 0.20),
    "mid_rate_range": (0.20, 0.80),
    "finish_rate_range": (0.80, 1.00),
    "distribution_weights": {
        "bounce": 0.40,
        "mid": 0.20,
        "finish": 0.40
    }
}
RATINGS_CONFIG = {
    "min_rating": 1,
    "max_rating": 5
}
PAYMENT_CONFIG = {
    "payment_methods": ["Credit Card", "Debit Card", "Gift Card"],
    "payment_statuses": ["Success", "Pending", "Failed"],
    "payment_amount_by_subscription": {
        "Standard-With-Ads": 8.99,
        "Standard": 19.99,
        "Premium": 26.99,
    },
}

# ==========================================
# 2. LOGIC & FUNCTIONS
# ==========================================

def write_parquet(
    df: pd.DataFrame, output_path: str, partition_cols: list[str] | None = None
) -> None:
    """Write a DataFrame to Parquet format, optionally partitioning by specified columns."""

    # Create a shallow copy to prevent side effects on the source DataFrame
    df_storage = df.copy()

    # Identify and downcast any nanosecond timestamp columns safely
    for col in df_storage.columns:
        if pd.api.types.is_datetime64_ns_dtype(df_storage[col]):
            # .dt accessor works perfectly here because df_storage[col] is a Series
            df_storage[col] = df_storage[col].astype("datetime64[us]")

    # Convert the cleaned DataFrame into a PyArrow Table
    tbl = pa.Table.from_pandas(df_storage)
    # tbl = pa.Table.from_pandas(df)

    if partition_cols:
        hive_partitioning = ds.partitioning(
            schema=pa.schema([tbl.schema.field(col) for col in partition_cols]),
            flavor="hive",
        )

        ds.write_dataset(
            tbl,
            base_dir=output_path,
            format="parquet",
            partitioning=hive_partitioning,
            max_rows_per_group=10_000,
        )
    else:
        pq.write_table(tbl, output_path)


def generate_users(
    n_users: int,
    user_config: dict,
    user_base_date: pd.Timestamp,
    days_history: int,
    schema_change_date: pd.Timestamp,
) -> list[pd.DataFrame]:
    """Generate an 'Old Pool' of users data with a monthly distribution where

    all timestamps are shifted into the deep past (strictly older than
    days_history).

    This ensures users exist before any transaction data (like playbacks) is
    generated.
    """
    # APPROACH UPDATE: Shift the generation window backwards by an extra 'days_history'
    # This places the pool between (base_date - 2*days_history) and (base_date - days_history)
    start_date_pool = user_base_date - pd.to_timedelta(days_history, unit="D")

    # Generate months specifically for this older historical pool window
    months = pd.date_range(
        start=start_date_pool, periods=days_history // 30, freq="MS"
    )
    base_rows_per_month = n_users // len(months)
    remaining_rows = n_users - (base_rows_per_month * len(months))

    all_months_dfs = []
    current_id_start = 1

    for i, month in enumerate(months):
        month_metadata = month.strftime("%Y-%m")
        current_month_rows = base_rows_per_month
        if i == len(months) - 1:
            current_month_rows += remaining_rows

        # Generate registration timestamps within the current old month block
        days_in_current_month = month.days_in_month
        max_seconds = days_in_current_month * 24 * 60 * 60
        random_offsets = np.random.randint(0, max_seconds, current_month_rows)
        signup_timestamps = month + pd.to_timedelta(random_offsets, unit="s")

        # Base data dictionary (Missing the 'gender' column completely)
        data = {
            "user_id": np.arange(
                current_id_start, current_id_start + current_month_rows
            ),
            "age": np.random.randint(
                user_config["min_age"],
                user_config["max_age"] + 1,
                current_month_rows,
            ),
            "subscription_type": np.random.choice(
                user_config["subscription_types"], current_month_rows
            ),
            "signup_ts": signup_timestamps,
        }

        # SCHEMA EVOLUTION: Evaluated against the old historical months
        if signup_timestamps[0] >= schema_change_date:
            data["gender"] = np.random.choice(
                user_config["genders"], current_month_rows
            )

        current_id_start += current_month_rows

        # Convert to DataFrame and preserve monthly metadata
        df_month = pd.DataFrame(data)
        df_month.attrs["month_metadata"] = month_metadata
        all_months_dfs.append(df_month)

    print("1. Old Pool User data (Guaranteed older than active tracking window):")
    print("Before schema evolution (No gender column):")
    print(all_months_dfs[0].head(5), "\n")
    print("After schema evolution (Includes gender column):")
    print(all_months_dfs[-1].head(5), "\n")

    total_rows = sum(len(df) for df in all_months_dfs)
    print(f"Total number of old users pool generated: {total_rows} \n")
    return all_months_dfs

def generate_movies(
    n_movies: int,
    movie_config: dict,
    movie_base_date: pd.Timestamp,
    days_history: int,
    skew_genre: str,
    skew_ratio_genre: float,
) -> pd.DataFrame:
    """Generate an 'Old Pool' of movies data vectorially, where all creation

    timestamps are strictly older than days_history.

    This guarantees every movie exists prior to any transactional playback
    events.
    """
    # 1. Calculate skewed probabilities cleanly using NumPy
    genres = movie_config["genres"]
    other_genres = [g for g in genres if g != skew_genre]

    other_probs = np.random.random(len(other_genres))
    other_probs = (other_probs / other_probs.sum()) * (1 - skew_ratio_genre)

    genre_probs = []
    other_idx = 0
    for g in genres:
        if g == skew_genre:
            genre_probs.append(skew_ratio_genre)
        else:
            genre_probs.append(other_probs[other_idx])
            other_idx += 1

    start_date_pool = movie_base_date - pd.to_timedelta(days_history, unit="D")

    total_seconds_history = days_history * 24 * 60 * 60
    random_offsets = np.random.randint(0, total_seconds_history, n_movies)
    created_at_timestamps = start_date_pool + pd.to_timedelta(
        random_offsets, unit="s"
    )

    # 3. Construct the entire DataFrame globally in one shot
    data = {
        "movie_id": np.arange(1, n_movies + 1),
        "genre": np.random.choice(genres, n_movies, p=genre_probs),
        "runtime_seconds": np.random.randint(
            movie_config["min_runtime_seconds"],
            movie_config["max_runtime_seconds"] + 1,
            n_movies,
        ),
        "language": np.random.choice(movie_config["languages"], n_movies),
        "release_year": np.random.randint(
            movie_config["start_release_year"],
            movie_config["end_release_year"] + 1,
            n_movies,
        ),
        "created_at": created_at_timestamps,
    }

    movies_df = pd.DataFrame(data)

    print("2. Old Pool Movie data (Guaranteed older than active tracking window):")
    print(movies_df.head(5), "\n")
    print(f"Total number of old movies pool generated: {len(movies_df)} \n")

    return movies_df

def generate_playbacks(
    n_playbacks: int,
    n_users: int,
    movies_df: pd.DataFrame,
    playback_config: dict,
    playback_base_date: pd.Timestamp,
    days_history: int,
    duplicate_rate: float = 0.05,
) -> pd.DataFrame:
    """Generate reproducible playback logs with a bimodal completion rate distribution and microsecond timing precision."""
    
    # 1. Generate the bimodal completion rates cleanly using vectorization
    bounce_rng = playback_config["bounce_rate_range"]
    mid_rng = playback_config["mid_rate_range"]
    finish_rng = playback_config["finish_rate_range"]
    weights = playback_config["distribution_weights"]

    bounce_rates = np.random.uniform(bounce_rng[0], bounce_rng[1], size=n_playbacks)
    mid_rates = np.random.uniform(mid_rng[0], mid_rng[1], size=n_playbacks)
    finish_rates = np.random.uniform(finish_rng[0], finish_rng[1], size=n_playbacks)

    group_selector = np.random.rand(n_playbacks)
    completion_rates = np.where(
        group_selector < weights["bounce"],
        bounce_rates,
        np.where(
            group_selector < (weights["bounce"] + weights["mid"]), 
            mid_rates, 
            finish_rates
        ),
    )

    # 2. Generate high cardinality playbacks using replace=True
    df_playbacks = pd.DataFrame(
        {
            "playback_id": np.arange(1, n_playbacks + 1),
            "user_id": np.random.choice(np.arange(1, n_users + 1), n_playbacks, replace=False),
            "movie_id": np.random.choice(np.arange(1, len(movies_df) + 1), n_playbacks, replace=False),
        }
    )

    # Merge runtimes temporarily to accurately evaluate playback durations
    df_playbacks = df_playbacks.merge(
        movies_df[["movie_id", "runtime_seconds"]], on="movie_id", how="left"
    )

    # 3. Generate random execution timestamps within the exact historical footprint
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_playbacks)
    
    df_playbacks["click_ts"] = playback_base_date - pd.to_timedelta(random_seconds_offset, unit="s")
    df_playbacks["start_ts"] = df_playbacks["click_ts"] + pd.to_timedelta(
        np.random.randint(0, 15, n_playbacks), unit="s"
    )
    
    df_playbacks["completion_rate"] = completion_rates
    df_playbacks["duration_watched_seconds"] = (
        df_playbacks["runtime_seconds"] * df_playbacks["completion_rate"]
    ).astype(int)
    
    df_playbacks["end_ts"] = df_playbacks["start_ts"] + pd.to_timedelta(
        df_playbacks["duration_watched_seconds"], unit="s"
    )
    df_playbacks["playback_date"] = df_playbacks["click_ts"].dt.strftime("%Y-%m-%d")
    
    df_playbacks.drop(columns=["runtime_seconds"], inplace=True)

    # 4. Inject systemic duplicate entries using identical transaction fingerprints
    n_duplicates = int(n_playbacks * duplicate_rate)
    if n_duplicates > 0:
        duplicates = df_playbacks.sample(n_duplicates, replace=False).copy()
        duplicates["playback_id"] = np.arange(
            n_playbacks + 1, n_playbacks + n_duplicates + 1
        )
        df_playbacks = pd.concat([df_playbacks, duplicates], ignore_index=True)

    print("3. Playback data:")
    print(df_playbacks.head(5), "\n")
    print(f"Total number of playback transactions generated: {len(df_playbacks)} \n")
    
    return df_playbacks

def generate_ratings(
    n_ratings: int,
    n_users: int,
    movies_df: pd.DataFrame,
    ratings_config: dict,
    rating_base_date: pd.Timestamp,
    days_history: int,
) -> pd.DataFrame:
    """Generate ratings logs distributed uniformly across history with explicit timestamps and partitioned dates."""
    
    # Calculate total history window in seconds
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_ratings)

    # Generate transaction timestamps moving backward from base_date
    rating_ts = rating_base_date - pd.to_timedelta(random_seconds_offset, unit="s")

    # Combine generated monthly movie datasets to calculate the length
    total_movies = len(movies_df)
    
    # Extract config limits (+1 to max_rating for inclusion in np.random.randint)
    low_rate = ratings_config["min_rating"]
    high_rate = ratings_config["max_rating"] + 1

    # Construct unified ratings DataFrame
    df_ratings = pd.DataFrame(
        {
            "rating_id": np.arange(1, n_ratings + 1),
            "user_id": np.random.choice(np.arange(1, n_users + 1), n_ratings, replace=True),
            "movie_id": np.random.choice(np.arange(1, total_movies + 1), n_ratings, replace=True),
            "rating": np.random.randint(low_rate, high_rate, size=n_ratings),
            "rating_ts": rating_ts,
            "rating_date": rating_ts.strftime("%Y-%m-%d"),
        }
    )

    print("4. Ratings data:")
    print(df_ratings.head(5), "\n")
    print(f"Total number of ratings generated: {len(df_ratings)}\n")

    return df_ratings

def generate_payments(
    n_payments: int,
    users_dfs: list[pd.DataFrame],
    payment_config: dict,
    payment_base_date: pd.Timestamp,
    days_history: int,
) -> pd.DataFrame:
    """Generate payment logs and map transaction amounts based on user subscription types."""
    
    # 1. Simplify timestamp generation using a clean microsecond/second offset
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_payments)
    payment_ts = payment_base_date - pd.to_timedelta(random_seconds_offset, unit="s")
    
    # concatinate the list of users df into a users_df
    df_users = pd.concat( users_dfs, axis=0, join='outer', ignore_index=True, sort=False)

    # 2. Construct the base payments DataFrame
    df_payments = pd.DataFrame(
        {
            "payment_id": np.arange(1, n_payments + 1),
            "user_id": np.random.choice(np.arange(1, len(df_users) + 1), n_payments, replace=True),
            "payment_method": np.random.choice(payment_config["payment_methods"], n_payments),
            "payment_status": np.random.choice(payment_config["payment_statuses"], n_payments),
            "payment_ts": payment_ts,
        }
    )

    # 3. Merge to fetch subscription tiers and map corresponding amounts
    df_payments = df_payments.merge(
        df_users[["user_id", "subscription_type"]], on="user_id", how="left"
    )
    df_payments["amount"] = df_payments["subscription_type"].map(
        payment_config["payment_amount_by_subscription"]
    )
    
    # 4. Clean up temporary columns and format partition dates
    df_payments.drop(columns=["subscription_type"], inplace=True)
    df_payments["payment_date"] = df_payments["payment_ts"].dt.strftime("%Y-%m-%d")

    print("5. Payments data:")
    print(df_payments.head(5), "\n")
    print(f"Total number of payments generated: {len(df_payments)}\n")

    return df_payments

def offline_data_generator(
    n_users: int,
    n_movies: int,
    n_playbacks: int,
    n_ratings: int,
    n_payment_attempts: int,
    base_date: pd.Timestamp,
    historical_pool_base_date: pd.Timestamp,
    days_history: int,
    historical_pool_schema_change_date: pd.Timestamp,
    skew_genre: str,
    skew_ratio_genre: float,
    duplicate_rate: float,
) -> tuple[list[pd.DataFrame], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Orchestrates the generation of all synthetic datasets."""
    
    users_dfs = generate_users(
        n_users=n_users,
        user_config=USER_CONFIG,
        user_base_date=historical_pool_base_date,
        days_history=days_history,
        schema_change_date=historical_pool_schema_change_date,
    )

    movies_df = generate_movies(
        n_movies=n_movies,
        movie_config=MOVIE_CONFIG,
        movie_base_date=historical_pool_base_date,
        days_history=days_history,
        skew_genre=skew_genre,
        skew_ratio_genre=skew_ratio_genre,
    )

    playbacks_df = generate_playbacks(
        n_playbacks=n_playbacks,
        n_users=n_users,
        movies_df=movies_df,
        playback_config=PLAYBACK_CONFIG,
        playback_base_date=base_date,
        days_history=days_history,
        duplicate_rate=duplicate_rate,
    )

    ratings_df = generate_ratings(
        n_ratings=n_ratings,
        n_users=n_users,
        movies_df=movies_df,
        ratings_config=RATINGS_CONFIG,
        rating_base_date=base_date,
        days_history=days_history,
    )

    payments_df = generate_payments(
        n_payments=n_payment_attempts,
        users_dfs=users_dfs,
        payment_config=PAYMENT_CONFIG,
        payment_base_date=base_date,
        days_history=days_history,
    )

    return users_dfs, movies_df, playbacks_df, ratings_df, payments_df


def write_offline_data(
    users_dfs: list[pd.DataFrame],
    movies_df: pd.DataFrame,
    playbacks_df: pd.DataFrame,
    ratings_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    base_output_path: "Path" = OFFLINE_DATA_PATH
) -> None:
    """Handles directory wiping, creation, and saving datasets to Parquet."""
    
    if base_output_path.exists():
        shutil.rmtree(base_output_path)
        print("Folder contents wiped. Empty folder preserved.")        

    base_output_path.mkdir(parents=True, exist_ok=True)

    # Set up and create output directories safely using pathlib
    users_output_dir = base_output_path / "users"
    movies_output_path = base_output_path / "movies.parquet"
    playbacks_output_path = base_output_path / "playbacks.parquet"
    ratings_output_path = base_output_path / "ratings.parquet"
    payments_output_path = base_output_path / "payments.parquet"

    # check if the users_output_dir exist
    users_output_dir.mkdir(exist_ok=True)
    
    # Write users data
    for df_month in users_dfs:
        month_str = df_month.attrs["month_metadata"]
        file_name = f"movies_{month_str}.parquet"
        output_file_path = users_output_dir / file_name
        
        write_parquet(df_month, str(output_file_path))

    print(f"Successfully generated and saved users data to {users_output_dir}")

    write_parquet(movies_df, str(movies_output_path))
    print(f"Successfully generated and saved monthly movie files to {movies_output_path}")

    # Write Hive-partitioned DataFrames
    write_parquet(
        df=playbacks_df, 
        output_path=str(playbacks_output_path), 
    )
    print(f"Successfully saved playbacks data to {playbacks_output_path}")

    write_parquet(
        df=ratings_df, 
        output_path=str(ratings_output_path), 
    )
    print(f"Successfully saved ratings data to {ratings_output_path}")

    write_parquet(
        df=payments_df, 
        output_path=str(payments_output_path), 
        # partition_cols=["payment_date"]
    )
    print(f"Successfully saved payments data to {payments_output_path}")


# if __name__ == "__main__":
    
#     # Initialize environment
#     np.random.seed(RANDOM_SEED)

#     # Configuration parameters
#     EXECUTION_PARAMS = {
#         "n_users": 100_000,
#         "n_movies": 100_000,
#         "n_playbacks": 100_000,
#         "n_ratings": 50_000,
#         "n_payment_attempts": 50_000,
#         "base_date": pd.Timestamp("2026-06-07"),
#         "days_history": 180,
#         "schema_change_date": pd.Timestamp("2026-04-07"),
#         "skew_genre": "Drama",
#         "skew_ratio_genre": 0.6,
#         "duplicate_rate": 0.05,
#     }

#     # Generate data
#     generated_data = offline_data_generator(**EXECUTION_PARAMS)

#     # Unpack and write data to disk
#     write_offline_data(*generated_data, base_output_path=OFFLINE_DATA_PATH)

#     os._exit(0)




