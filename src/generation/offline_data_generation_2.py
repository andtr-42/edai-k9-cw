import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# ==========================================
# 1. CONFIGURATION
# ==========================================
OFFLINE_DATA_DIR = "../../data/raw/offline_2/"
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

PAYMENT_CONFIG = {
    "payment_methods": ["Credit Card", "Debit Card", "Gift Card"],
    "payment_statuses": ["Success", "Pending", "Failed"],
    "amount_by_subscription": {
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
    tbl = pa.Table.from_pandas(df)

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
    print(f"Total number of movies generated: {total_rows}")
    return all_months_dfs


if __name__ == "__main__":
    # Initialize environment
    np.random.seed(RANDOM_SEED)
    os.makedirs(OFFLINE_DATA_DIR, exist_ok=True)

    # Execution parameters
    n_users = 100_000
    n_movies = 100_00
    base_date = pd.Timestamp("2026-06-07")  # Set static date for reproducible
    days_history = 180
    schema_change_date = pd.Timestamp("2026-04-07") # Set static date for reproducible
    skew_genre="Drama"
    skew_ratio_genre = 0.6


    # Generate users data
    users_df = generate_users(
        n_users=n_users,
        user_config=USER_CONFIG,
        base_date=base_date,
        days_history=days_history,
    )

    # Write to user data to output path
    # output_users_path = os.path.join(OFFLINE_DATA_DIR, "users.parquet")
    # write_parquet(users_df, output_users_path)

    # print(f"Successfully generated and saved {n_users} users to {output_users_path}")

    # Generate movies data
    movies_df = generate_movies(
        n_movies=n_movies,
        movie_config=MOVIE_CONFIG,
        base_date = base_date,
        days_history = days_history,
        skew_genre = skew_genre,
        skew_ratio_genre = skew_ratio_genre,
        schema_change_date = schema_change_date,
    )
