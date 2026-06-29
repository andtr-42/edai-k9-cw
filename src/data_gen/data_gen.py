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
from pathlib import Path

from src.config import OFFLINE_DATA_PATH, STREAMING_DATA_PATH
from src.data_gen.offline_data_gen import offline_data_generator, write_offline_data
from src.data_gen.streaming_data_gen import streaming_data_generator, save_events_to_jsonl

def data_generator(
    # Core environments
    random_seed: int,
    offline_data_path: Path,
    streaming_data_path: Path,
    
    # Offline Parameters
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
    
    # Streaming Parameters
    streaming_base_date: pd.Timestamp,
    hours_history: int,
    base_events_per_min: int,
    burst_multiplier: int,
    burst_windows: list[str],
    late_arrival_rate: float,
    late_delay_min_max: list[int],
    streaming_duplicate_rate: float,
    duplicate_delay_min_max: list[int],
) -> None:
    """Orchestrates the end-to-end generation of all synthetic datasets."""

    print("==================================================")
    print("🚀 STARTING SYNTHETIC DATA GENERATION PIPELINE")
    print("==================================================\n")

    # Set global seed for absolute reproducibility across both engines
    np.random.seed(random_seed)

    # Ensure output directories exist
    offline_data_path.mkdir(parents=True, exist_ok=True)
    streaming_data_path.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # Phase 1: Offline Data Generation
    # ---------------------------------------------------------
    print("--- [PHASE 1] GENERATING OFFLINE DATA ---")
    generated_offline_data = offline_data_generator(
        n_users=n_users,
        n_movies=n_movies,
        n_playbacks=n_playbacks,
        n_ratings=n_ratings,
        n_payment_attempts=n_payment_attempts,
        base_date=offline_base_date,
        days_history=days_history,
        schema_change_date=schema_change_date,
        skew_genre=skew_genre,
        skew_ratio_genre=skew_ratio_genre,
        duplicate_rate=offline_duplicate_rate,
    )

    write_offline_data(
        *generated_offline_data, 
        base_output_path=offline_data_path
    )
    print("✅ Offline data generation complete.\n")

    # ---------------------------------------------------------
    # Phase 2: Streaming Data Generation
    # ---------------------------------------------------------
    print("--- [PHASE 2] GENERATING STREAMING DATA ---")
    df_events = streaming_data_generator(
        offline_data_path=offline_data_path,
        base_date=streaming_base_date,
        hours_history=hours_history,
        base_events_per_min=base_events_per_min,
        burst_multiplier=burst_multiplier,
        burst_windows=burst_windows,
        late_arrival_rate=late_arrival_rate,
        late_delay_min_max=late_delay_min_max,
        duplicate_rate=streaming_duplicate_rate,
        duplicate_delay_min_max=duplicate_delay_min_max,
    )

    streaming_output_file = streaming_data_path / "streaming_events.jsonl"
    save_events_to_jsonl(df_events, streaming_output_file)
    print("✅ Streaming data generation complete.\n")

    print("==================================================")
    print("🎉 ALL DATA SUCCESSFULLY GENERATED AND SAVED!")
    print("==================================================")


if __name__ == "__main__":
    
    # Unified execution parameters mapping
    MASTER_CONFIG = {
        # Core
        "random_seed": 42,
        "offline_data_path": OFFLINE_DATA_PATH,
        "streaming_data_path": STREAMING_DATA_PATH,
        
        # Offline Configuration
        "n_users": 100_000,
        "n_movies": 100_000,
        "n_playbacks": 100_000,
        "n_ratings": 50_000,
        "n_payment_attempts": 50_000,
        "offline_base_date": pd.Timestamp("2026-06-07"), # static date for reproducible
        "days_history": 180,
        "schema_change_date": pd.Timestamp("2026-04-07"),
        "skew_genre": "Drama",
        "skew_ratio_genre": 0.9,
        "offline_duplicate_rate": 0.05,
        
        # Streaming Configuration
        "streaming_base_date": pd.Timestamp("2026-06-08"), # static data for reproducible
        "hours_history": 24,
        "base_events_per_min": 100,
        "burst_multiplier": 30,
        "burst_windows": ["12:00-12:20", "20:00-20:20"],
        "late_arrival_rate": 0.12,
        "late_delay_min_max": [5, 45], # in seconds
        "streaming_duplicate_rate": 0.02,
        "duplicate_delay_min_max": [60, 180], # in seconds
    }

    data_generator(**MASTER_CONFIG)