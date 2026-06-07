import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

OUTPUT_DIR_OFFLINE = "../data/raw/offline/"
os.makedirs(OUTPUT_DIR_OFFLINE, exist_ok=True)
np.random.seed(42)

N_USERS = 100_000
N_MOVIES = 100_000
N_PLAYBACKS = 100_000
N_RATINGS = 50_000
N_PAYMENTS = 50_000
DAYS_HISTORY = 180

# users table
GENDERS = ["M", "F", "O"]  # 3 genders
MAX_AGE = 80  # max age of users
MIN_AGE = 16  # min age of users
SUBSCRIPTION_TYPES = [
    "Standard-With-Ads",
    "Standard",
    "Premium",
]  # 3 subscription types
# movies table
GENRES = [
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
]  # 10 genres
COUNTRIES = [
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
]  # 10 countries
LANGUAGES = [
    "English",
    "French",
    "Vietnamese",
    "Japanese",
    "Portuguese",
    "Italian",
    "Korean",
    "Chinese",
    "Spanish",
]  # 9 languages
MAX_MOVIE_RUNTIME = 3 * 60 * 60  # 3 hours in seconds
LANGUAGE_BY_COUNTRY = {
    "USA": "English",
    "UK": "English",
    "France": "French",
    "Vietnam": "Vietnamese",
    "Japan": "Japanese",
    "Portugal": "Portuguese",
    "Italy": "Italian",
    "South Korea": "Korean",
    "China": "Chinese",
    "Spain": "Spanish",
}
# payment table
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Gift Card"]  # 3 payment methods
PAYMENT_STATUSES = ["Success", "Pending", "Failed"]  # 3 payment statuses
PAYMENT_AMOUNT_BY_SUBSCRIPTION = {
    "Standard-With-Ads": 8.99,
    "Standard": 19.99,
    "Premium": 26.99,
}


def _write(df: pd.DataFrame, path: str, partition_cols: list = None) -> None:
    tbl = pa.Table.from_pandas(df)
    if partition_cols:

        hive_partitioning = ds.partitioning(
            schema=pa.schema([tbl.schema.field(col) for col in partition_cols]),
            flavor="hive"
        )

        ds.write_dataset(
            tbl,
            base_dir=path,
            format="parquet",
            partitioning=hive_partitioning,
            max_rows_per_group=10_000,
        )
    else:
        pq.write_table(tbl, path)


def generate_users(n_users: int, days_history: int = 180) -> None:
    df = pd.DataFrame(
        {
            "user_id": np.arange(1, n_users + 1),
            "gender": np.random.choice(GENDERS, n_users),
            "age": np.random.randint(MIN_AGE, MAX_AGE + 1, n_users),
            "subscription_type": np.random.choice(SUBSCRIPTION_TYPES, n_users),
            "signup_date": pd.Timestamp.now(tz="UTC")
            - pd.to_timedelta(
                np.random.randint(0, days_history + 1, n_users), unit="D"
            ),
        }
    )
    _write(df, os.path.join(OUTPUT_DIR_OFFLINE, "users.parquet"))
    print(
        f"users.parquet  — {n_users:,} rows, uniform distribution, all columns present"
    )

def generate_movies(
    n_movies: int,
    days_history: int,
    skew_genre: str,
    skew_ratio_genre: float,
    schema_change_date: pd.Timestamp,
) -> None:
    """Generate movies data with skew distribution for genres and a schema change after a certain date."""

    # create skewed genre distribution
    remaining_ratio = 1 - skew_ratio_genre
    other_genres = [g for g in GENRES if g != skew_genre]
    other_genres_probs = np.random.random(len(other_genres))
    normalized_other_genres_probs = list(
        (other_genres_probs / other_genres_probs.sum()) * remaining_ratio
    )
    index_skew_genre = GENRES.index(skew_genre)
    genre_probs = normalized_other_genres_probs.copy()
    genre_probs.insert(index_skew_genre, skew_ratio_genre)

    # partition by month to simulate schema change over time
    start_date = pd.Timestamp.now(tz="UTC") - pd.to_timedelta(days_history, unit="D")
    months = pd.date_range(start=start_date, periods=days_history // 30, freq="MS")
    out = f"{OUTPUT_DIR_OFFLINE}/movies_{skew_ratio_genre}_{skew_genre.lower()}_{schema_change_date.strftime('%Y%m%d')}"
    os.makedirs(out, exist_ok=True)
    rows_per_month = n_movies // len(months)
    remaining_rows = n_movies - (rows_per_month * len(months))

    for i, month in enumerate(months):
        if i == len(months) - 1:  # last month takes the remaining rows
            rows_per_month += remaining_rows

        label = month.strftime("%Y")
        data = {
            "movie_id": np.arange(i * rows_per_month + 1, (i + 1) * rows_per_month + 1),
            "genre": np.random.choice(GENRES, rows_per_month, p=genre_probs),
            "runtime_seconds": np.random.randint(
                30, MAX_MOVIE_RUNTIME + 1, rows_per_month
            ),
            "language": np.random.choice(LANGUAGES, rows_per_month),
            "release_year": label,
        }
        if month >= schema_change_date:
            chosen_countries = np.random.choice(COUNTRIES, size=rows_per_month)
            chosen_languages = [
                LANGUAGE_BY_COUNTRY[country] for country in chosen_countries
            ]

            # Assign explicitly to your month's data container
            data["country"] = list(chosen_countries)
            data["language"] = chosen_languages

        df = pd.DataFrame(data)
        _write(df, os.path.join(out, f"movies_{month.strftime('%Y-%m')}.parquet"))
    print(
        f"movies_{skew_genre}_{skew_ratio_genre}_{schema_change_date.strftime('%Y%m%d')}  — {n_movies:,} rows, skewed genre distribution, schema change after {schema_change_date.strftime('%Y-%m-%d')}"
    )


def generate_playbacks(
    n_playbacks: int,
    n_users: int,
    n_movies: int,
    days_history: int,
    schema_change_date: pd.Timestamp,
) -> None:

    # completion rate skewed bimodal distribution 40% bounce (0-20%), 20% mid (20-80%), 40% finish (80-100%)
    bounce_rates = np.random.uniform(0.01, 0.20, size=n_playbacks)
    finish_rates = np.random.uniform(0.80, 1.00, size=n_playbacks)
    mid_rates = np.random.uniform(0.20, 0.80, size=n_playbacks)

    group_selector = np.random.rand(n_playbacks)

    completion_rates = np.where(
        group_selector < 0.40,  # First 40% goes to bounce
        bounce_rates,
        np.where(
            group_selector < 0.60, mid_rates, finish_rates
        ),  # Next 20% (0.40 to 0.60) goes to mid, rest to finish
    )

    # read movies to get runtime
    df_movies = pd.read_parquet(
        os.path.join(
            OUTPUT_DIR_OFFLINE,
            f"movies_0.6_drama_{schema_change_date.strftime('%Y%m%d')}",
        )
    )

    df_playbacks = pd.DataFrame(
        {
            "playback_id": np.arange(1, n_playbacks + 1),
            "user_id": np.random.choice(n_users, n_playbacks, replace=False),
            "movie_id": np.random.choice(
                df_movies["movie_id"].values, n_playbacks, replace=False
            ),
        }
    )

    df_playbacks = df_playbacks.merge(
        df_movies[["movie_id", "runtime_seconds"]], on="movie_id", how="left"
    )
    df_playbacks["click_ts"] = (
        pd.Timestamp.now(tz="UTC")
        - pd.to_timedelta(np.random.randint(0, days_history + 1, n_playbacks), unit="D")
        + pd.to_timedelta(np.random.randint(0, 24, n_playbacks), unit="h")
        + pd.to_timedelta(np.random.randint(0, 60, n_playbacks), unit="m")
    )
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
    # remove runtime_seconds as it's not needed in playbacks table
    df_playbacks.drop(columns=["runtime_seconds"], inplace=True)

    # generate duplicates for 5% of playbacks with new playback_id but same user_id, movie_id, click_ts
    n_duplicates = int(n_playbacks * 0.05)
    duplicates = df_playbacks.sample(n_duplicates, replace=False)
    duplicates["playback_id"] = np.arange(
        n_playbacks + 1, n_playbacks + n_duplicates + 1
    )
    df_playbacks = pd.concat([df_playbacks, duplicates], ignore_index=True)

    out = f"{OUTPUT_DIR_OFFLINE}/playbacks"
    os.makedirs(out, exist_ok=True)

    _write(df_playbacks, out, partition_cols=["playback_date"])
    print(
        f"playbacks  — {n_playbacks:,} rows, skewed completion rate distribution, partitioned by click_ts"
    )


def generate_ratings(
    n_ratings: int,
    n_users: int,
    n_movies: int,
    days_history: int,
    schema_change_date: pd.Timestamp,
) -> None:

    # partition by month to simulate schema change over time
    start_date = pd.Timestamp.now(tz="UTC") - pd.to_timedelta(days_history, unit="D")
    months = pd.date_range(start=start_date, periods=days_history // 30, freq="MS")
    out = f"{OUTPUT_DIR_OFFLINE}/ratings_{schema_change_date.strftime('%Y%m%d')}"
    os.makedirs(out, exist_ok=True)
    rows_per_month = n_ratings // len(months)
    remaining_rows = n_ratings - (rows_per_month * len(months))

    df = pd.DataFrame()

    for i, month in enumerate(months):
        if i == len(months) - 1:  # last month takes the remaining rows
            rows_per_month = rows_per_month + remaining_rows

        data = {
            "rating_id": np.arange(
                i * rows_per_month + 1, (i + 1) * rows_per_month + 1
            ),
            "user_id": np.random.choice(n_users, rows_per_month, replace=True),
            "movie_id": np.random.choice(n_movies, rows_per_month, replace=True),
            "rating": np.random.randint(
                1, 6, size=rows_per_month
            ),  # ratings between 1 and 5
        }
        if month >= schema_change_date:
            data["rating_ts"] = (
                month
                + pd.to_timedelta(np.random.randint(0, 30, rows_per_month), unit="D")
                + pd.to_timedelta(np.random.randint(0, 24, rows_per_month), unit="h")
                + pd.to_timedelta(np.random.randint(0, 60, rows_per_month), unit="m")
            )
        df_month = pd.DataFrame(data)
        df = pd.concat([df, df_month], ignore_index=True)

    earliest_rating_date = df["rating_ts"].min()
    sentinel_date = earliest_rating_date - pd.to_timedelta(1, unit="D")
    df["rating_ts"] = df["rating_ts"].fillna(sentinel_date)
    df["rating_date"] = df["rating_ts"].dt.strftime("%Y-%m-%d")

    out = f"{OUTPUT_DIR_OFFLINE}/ratings_{schema_change_date.strftime('%Y%m%d')}"
    os.makedirs(out, exist_ok=True)

    _write(df, out, partition_cols=["rating_date"])
    print(
        f"ratings  — {n_ratings:,} rows, ratings between 1 and 5, schema change after {schema_change_date.strftime('%Y-%m-%d')}, partitioned by rating_date"
    )


def generate_payments(n_payments: int, n_users: int, days_history: int) -> None:

    df_users = pd.read_parquet(os.path.join(OUTPUT_DIR_OFFLINE, "users.parquet"))

    df_payments = pd.DataFrame(
        {
            "payment_id": np.arange(1, n_payments + 1),
            "user_id": np.random.choice(n_users, n_payments, replace=True),
            "payment_method": np.random.choice(PAYMENT_METHODS, n_payments),
            "payment_status": np.random.choice(PAYMENT_STATUSES, n_payments),
            "payment_ts": pd.Timestamp.now(tz="UTC")
            - pd.to_timedelta(
                np.random.randint(0, days_history + 1, n_payments), unit="D"
            )
            + pd.to_timedelta(np.random.randint(0, 24, n_payments), unit="h")
            + pd.to_timedelta(np.random.randint(0, 60, n_payments), unit="m"),
        }
    )

    df_payments = df_payments.merge(
        df_users[["user_id", "subscription_type"]], on="user_id", how="left"
    )
    df_payments["amount"] = df_payments["subscription_type"].map(
        PAYMENT_AMOUNT_BY_SUBSCRIPTION
    )
    df_payments.drop(columns=["subscription_type"], inplace=True)
    df_payments["payment_date"] = df_payments["payment_ts"].dt.strftime("%Y-%m-%d")

    out = f"{OUTPUT_DIR_OFFLINE}/payments"
    os.makedirs(out, exist_ok=True)

    _write(df_payments, out, partition_cols=["payment_date"])
    print(
        f"payments  — {n_payments:,} rows, uniform distribution across payment methods and statuses, partitioned by payment_date"
    )


def main(
    n_users,
    n_movies,
    days_history,
    skew_genre,
    skew_ratio_genre,
    n_playbacks,
    n_ratings,
    n_payments,
    schema_change_date,
):
    users_df = generate_users(n_users, days_history)
    movies_df = generate_movies(
        n_movies, days_history, skew_genre, skew_ratio_genre, schema_change_date
    )
    playbacks_df = generate_playbacks(
        n_playbacks, n_users, n_movies, days_history, schema_change_date
    )
    ratings_df = generate_ratings(
        n_ratings, n_users, n_movies, days_history, schema_change_date
    )
    payments_df = generate_payments(n_payments, n_users, days_history)
    os._exit(0)

    print("Data generation completed successfully.")


if __name__ == "__main__":
    main(
        N_USERS,
        N_MOVIES,
        DAYS_HISTORY,
        "Drama",
        0.6,
        N_PLAYBACKS,
        N_RATINGS,
        N_PAYMENTS,
        pd.Timestamp.now(tz="UTC") - pd.to_timedelta(90, unit="D"),
    )
