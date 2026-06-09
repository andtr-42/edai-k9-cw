import os
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# ==========================================
# 1. CONFIGURATION
# ==========================================
OFFLINE_DATA_DIR = "data/offline/"
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
    n_users: int, user_config: dict, base_date: pd.Timestamp, days_history: int
) -> pd.DataFrame:
    """Generate users data with specified configuration and historical distribution."""

    # 1. Calculate the total history window in seconds
    # 24 hours * 60 minutes * 60 seconds = 86,400 seconds per day
    total_seconds_history = (days_history + 1) * 24 * 60 * 60

    # 2. Generate a random number of seconds to subtract for each user
    random_seconds = np.random.randint(0, total_seconds_history, n_users)

    # 3. Subtract the random seconds from the base_date
    signup_ts = base_date - pd.to_timedelta(random_seconds, unit="s")

    df_users = pd.DataFrame(
        {
            "user_id": np.arange(1, n_users + 1),
            "gender": np.random.choice(
                user_config["genders"], n_users
            ),  # uniform distribution
            "age": np.random.randint(
                user_config["min_age"], user_config["max_age"] + 1, n_users
            ),
            "subscription_type": np.random.choice(
                user_config["subscription_types"], n_users
            ),
            "signup_ts": signup_ts,
        }
    )

    print("1. User data:")
    print(df_users.head(5), "\n")
    print("Total numbers of users generated: ", len(df_users), "\n")

    return df_users


def generate_movies(
    n_movies: int,
    movie_config: dict,
    base_date: pd.Timestamp,
    days_history: int,
    skew_genre: str,
    skew_ratio_genre: float,
    schema_change_date: pd.Timestamp,
) -> list[pd.DataFrame]:
    """Generate movies data with skew distribution for genres and a schema change after a certain date."""

    # 1 & 2. Calculate probabilities cleanly using NumPy
    genres = movie_config["genres"]
    other_genres = [g for g in genres if g != skew_genre]
    
    other_probs = np.random.random(len(other_genres))
    other_probs = (other_probs / other_probs.sum()) * (1 - skew_ratio_genre)
    
    # Reconstruct the full probability array matching the configuration order
    genre_probs = []
    other_idx = 0
    for g in genres:
        if g == skew_genre:
            genre_probs.append(skew_ratio_genre)
        else:
            genre_probs.append(other_probs[other_idx])
            other_idx += 1


    # 3. Handle tracking loop variables safely
    start_date = base_date - pd.to_timedelta(days_history, unit="D")
    months = pd.date_range(start=start_date, periods=days_history // 30, freq="MS")
    base_rows_per_month = n_movies // len(months)
    remaining_rows = n_movies - (base_rows_per_month * len(months))

    # 4. Generate the data per month
    all_months_dfs = []
    current_id_start = 1  # Track IDs sequentially to avoid math errors

    for i, month in enumerate(months):
        month_metadata = month.strftime("%Y-%m")
        # Determine exact row count for this specific iteration
        current_month_rows = base_rows_per_month
        if i == len(months) - 1:
            current_month_rows += remaining_rows

        # Generate the random number of current_month_rows seconds to create the created_ts
        days_in_current_month = month.days_in_month
        max_seconds = days_in_current_month * 24 * 60 * 60
        random_offsets = np.random.randint(0, max_seconds, current_month_rows)
        created_at_timestamps = month + pd.to_timedelta(random_offsets, unit="s")

        # Base data dictionary
        data = {
            "movie_id": np.arange(current_id_start, current_id_start + current_month_rows),
            "genre": np.random.choice(genres, current_month_rows, p=genre_probs),
            "runtime_seconds": np.random.randint(
                movie_config["min_runtime_seconds"], movie_config["max_runtime_seconds"] + 1, current_month_rows
            ),
            "language": np.random.choice(movie_config["languages"], current_month_rows),
            "release_year": np.random.randint(
                movie_config["start_release_year"], movie_config["end_release_year"] + 1, current_month_rows
            ),
            "created_at": created_at_timestamps
        }
        if month >= schema_change_date:
            chosen_countries = np.random.choice(movie_config["countries"], current_month_rows)
            # FIX: Use the explicit country-to-language mapping dictionary
            chosen_languages = [
                movie_config["language_by_country"][country] for country in chosen_countries
            ]
            data["country"] = chosen_countries
            data["language"] = chosen_languages

        current_id_start += current_month_rows

        # Convert the current dictionary into a temporary DataFrame
        df_month = pd.DataFrame(data)
        df_month.attrs["month_metadata"] = month_metadata
        all_months_dfs.append(df_month)

    print("2. Movie data: ")
    print("Before schema evolution: ")
    print(all_months_dfs[0].head(5), "\n")
    print("After schema evolution: ")
    print(all_months_dfs[-1].head(5), "\n")

    total_rows = sum(len(df) for df in all_months_dfs)
    print(f"Total number of movies generated: {total_rows} \n")
    return all_months_dfs

def generate_playbacks(
    n_playbacks: int,
    n_users: int,
    movies_dfs: list[pd.DataFrame],
    playback_config: dict,
    base_date: pd.Timestamp,
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

    # Combine generated monthly movie datasets to extract valid movie IDs and runtimes
    df_movies_all = pd.concat(movies_dfs, ignore_index=True)

    # 2. Generate high cardinality playbacks using replace=True
    df_playbacks = pd.DataFrame(
        {
            "playback_id": np.arange(1, n_playbacks + 1),
            "user_id": np.random.choice(np.arange(1, n_users + 1), n_playbacks, replace=False),
            "movie_id": np.random.choice(np.arange(1, len(df_movies_all) + 1), n_playbacks, replace=False),
        }
    )

    # Merge runtimes temporarily to accurately evaluate playback durations
    df_playbacks = df_playbacks.merge(
        df_movies_all[["movie_id", "runtime_seconds"]], on="movie_id", how="left"
    )

    # 3. Generate random execution timestamps within the exact historical footprint
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_playbacks)
    
    df_playbacks["click_ts"] = base_date - pd.to_timedelta(random_seconds_offset, unit="s")
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
    movies_dfs: list[pd.DataFrame],
    ratings_config: dict,
    base_date: pd.Timestamp,
    days_history: int,
) -> pd.DataFrame:
    """Generate ratings logs distributed uniformly across history with explicit timestamps and partitioned dates."""
    
    # Calculate total history window in seconds
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_ratings)

    # Generate transaction timestamps moving backward from base_date
    rating_ts = base_date - pd.to_timedelta(random_seconds_offset, unit="s")

    # Combine generated monthly movie datasets to calculate the length
    total_movies = sum(len(df) for df in movies_dfs)
    
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
    df_users: pd.DataFrame,
    payment_config: dict,
    base_date: pd.Timestamp,
    days_history: int,
) -> pd.DataFrame:
    """Generate payment logs and map transaction amounts based on user subscription types."""
    
    # 1. Simplify timestamp generation using a clean microsecond/second offset
    total_seconds_history = days_history * 24 * 60 * 60
    random_seconds_offset = np.random.randint(0, total_seconds_history, n_payments)
    payment_ts = base_date - pd.to_timedelta(random_seconds_offset, unit="s")

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

if __name__ == "__main__":

    if os.path.exists(OFFLINE_DATA_DIR):
        shutil.rmtree(OFFLINE_DATA_DIR)
        print("Folder contents wiped. Empty folder preserved.")
    else:
        print("Directory did not exist.")


    # Initialize environment
    np.random.seed(RANDOM_SEED)
    os.makedirs(OFFLINE_DATA_DIR, exist_ok=True)

    # Execution parameters
    n_users = 100_000
    n_movies = 100_000
    n_playbacks = 100_000
    n_ratings = 50_000
    n_payment_attempts = 50_000
    base_date = pd.Timestamp("2026-06-07")  # Set static date for reproducible
    days_history = 180
    schema_change_date = pd.Timestamp("2026-04-07") # Set static date for reproducible
    skew_genre="Drama"
    skew_ratio_genre = 0.6
    duplicate_rate = 0.05


    # DF GENERATIONS

    # Generate users data
    users_df = generate_users(
        n_users=n_users,
        user_config=USER_CONFIG,
        base_date=base_date,
        days_history=days_history,
    )

    # Generate movies data
    movies_dfs = generate_movies(
        n_movies=n_movies,
        movie_config=MOVIE_CONFIG,
        base_date = base_date,
        days_history = days_history,
        skew_genre = skew_genre,
        skew_ratio_genre = skew_ratio_genre,
        schema_change_date = schema_change_date,
    )

    # Generate playbacks data
    playbacks_df = generate_playbacks(
        n_playbacks=n_playbacks,
        n_users=n_users,
        movies_dfs=movies_dfs,
        playback_config=PLAYBACK_CONFIG,
        base_date=base_date,
        days_history=days_history,
    )

    ratings_df = generate_ratings(
        n_ratings=n_ratings,
        n_users=n_users,
        movies_dfs=movies_dfs,  # This uses the output from your existing generate_movies call
        ratings_config=RATINGS_CONFIG,
        base_date=base_date,
        days_history=days_history,
    )

    payments_df = generate_payments(
        n_payments=n_payment_attempts,
        df_users=users_df,              # Pass the DataFrame generated earlier in the script
        payment_config=PAYMENT_CONFIG,  # Pass the config dictionary from the top of your file
        base_date=base_date,
        days_history=days_history,
    )

    # WRITE DF TO PARQUET FILES

    # Set up output directories
    output_users_path = os.path.join(OFFLINE_DATA_DIR, "users.parquet")
    movies_output_dir = os.path.join(OFFLINE_DATA_DIR, "movies")
    playbacks_output_dir = os.path.join(OFFLINE_DATA_DIR, "playbacks")
    ratings_output_dir = os.path.join(OFFLINE_DATA_DIR, "ratings")
    payments_output_dir = os.path.join(OFFLINE_DATA_DIR, "payments")

    os.makedirs(movies_output_dir, exist_ok=True)
    os.makedirs(playbacks_output_dir, exist_ok=True)
    os.makedirs(ratings_output_dir, exist_ok=True)
    os.makedirs(payments_output_dir, exist_ok=True)
    
    # Write to user data to output path
    write_parquet(users_df, output_users_path)
    print(f"Successfully generated and saved users data to {output_users_path}")

    # Write movie data by loop through each monthly DataFrame and write it without partitioning
    for df_month in movies_dfs:
        # Retrieve the month metadata string stored in the DataFrame attributes
        month_str = df_month.attrs["month_metadata"]
        
        # Define a clean file name for each month (e.g., movies_2026-01.parquet)
        file_name = f"movies_{month_str}.parquet"
        output_file_path = os.path.join(movies_output_dir, file_name)
        
        # Call the existing write function without passing partition_cols
        write_parquet(df_month, output_file_path)

    print(f"Successfully generated and saved monthly movie files to {movies_output_dir}")

    # Write DataFrames to Parquet with Hive partitioning
    write_parquet(
        df=playbacks_df, 
        output_path=playbacks_output_dir, 
        partition_cols=["playback_date"]
    )
    print(f"Successfully saved playbacks data to {playbacks_output_dir}")

    write_parquet(
        df=ratings_df, 
        output_path=ratings_output_dir, 
        partition_cols=["rating_date"]
    )
    print(f"Successfully saved ratings data to {ratings_output_dir}")

    write_parquet(
        df=payments_df, 
        output_path=payments_output_dir, 
        partition_cols=["payment_date"]
    )
    print(f"Successfully saved payments data to {payments_output_dir}")

    os._exit(0)




