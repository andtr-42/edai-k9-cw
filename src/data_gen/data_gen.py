"""
Module: src.generation.main_data_gen
Type: Orchestrator (Data Generation Pipeline)

Description:
    Master execution script for generating both offline and streaming synthetic 
    datasets. Ensures data consistency by generating the offline demographic and 
    metadata universe first, then generating the corresponding streaming clickstream 
    events based on that foundation.

Execution:
    python3 -m src.data_gen.data_gen
"""

import numpy as np
import pandas as pd

from src.data_gen.offline_data_gen import offline_data_generator, insert_offline_data

def data_generator(
    random_seed: int,
    n_users: int,
    n_movies: int,
    n_playbacks: int,
    n_ratings: int,
    n_payment_attempts: int,
    offline_base_date: pd.Timestamp,
    days_history: int,
    schema_change_date: pd.Timestamp,
    skew_genre: str,
    skew_ratio_genre: float,
    offline_duplicate_rate: float,
) -> None:
    """Generates synthetic offline data and inserts it into the PostgreSQL data-source-storage DB."""

    print("==================================================")
    print("🚀 STARTING SYNTHETIC DATA GENERATION PIPELINE")
    print("==================================================\n")

    np.random.seed(random_seed)

    print("--- [PHASE 1] GENERATING OFFLINE DATA ---")
    generated_offline_data = offline_data_generator(
        n_users=n_users,
        n_movies=n_movies,
        n_playbacks=n_playbacks,
        n_ratings=n_ratings,
        n_payment_attempts=n_payment_attempts,
        base_date=offline_base_date,
        historical_pool_base_date=offline_base_date - pd.Timedelta(days=180),
        days_history=days_history,
        historical_pool_schema_change_date=schema_change_date - pd.Timedelta(days=180),
        skew_genre=skew_genre,
        skew_ratio_genre=skew_ratio_genre,
        duplicate_rate=offline_duplicate_rate,
    )

    insert_offline_data(*generated_offline_data)
    print("✅ Offline data inserted into data-source-storage.\n")

    # Phase 2 (streaming via CDC → Kafka → Flink) is deferred

    print("==================================================")
    print("🎉 DATA GENERATION COMPLETE!")
    print("==================================================")


if __name__ == "__main__":
    MASTER_CONFIG = {
        "random_seed": 42,
        "n_users": 100_000,
        "n_movies": 100_000,
        "n_playbacks": 100_000,
        "n_ratings": 50_000,
        "n_payment_attempts": 50_000,
        "offline_base_date": pd.Timestamp("2026-06-07"),
        "days_history": 180,
        "schema_change_date": pd.Timestamp("2026-04-07"),
        "skew_genre": "Drama",
        "skew_ratio_genre": 0.9,
        "offline_duplicate_rate": 0.05,
    }

    data_generator(**MASTER_CONFIG)