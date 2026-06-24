"""
Module: src.generation.validate_data_gen
Type: QA / Data Audit Script

Description:
    This script acts as a testing suite to validate that the synthetic data generated
    by the offline and streaming engines strictly adheres to the requested business 
    logic, skew configurations, schema evolutions, and streaming anomalies.

    It reads directly from the finalized Bronze layer output directories and prints
    a highly formatted ASCII Report Card designed for documentation screenshots.

Execution:
    python3 -m src.generation.validate_data_gen
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Import the configured paths from the central project config
from src.config import OFFLINE_DATA_PATH, STREAMING_DATA_PATH


def validate_movies_data(offline_path: Path, schema_change_date: pd.Timestamp) -> None:
    """
    Validates the 'movies' dataset for genre skew and schema evolution tracking.
    """
    print("\n" + "="*70)
    print(" 🎬 OFFLINE VALIDATION: MOVIES METADATA")
    print("="*70)

    # 1. Read Data (Handling Schema Drift)
    movies_path = offline_path / "movies"
    try:
        # Read files individually and concatenate to preserve evolving schemas
        parquet_files = list(movies_path.glob("*.parquet"))
        if not parquet_files:
            print(f"[ERROR] No parquet files found in {movies_path}")
            return
            
        dfs = [pd.read_parquet(file) for file in parquet_files]
        df_movies = pd.concat(dfs, ignore_index=True)
        
        # Format and explicitly sort the data chronologically
        df_movies['created_at'] = pd.to_datetime(df_movies['created_at'])
        df_movies = df_movies.sort_values(by='created_at').reset_index(drop=True)
        
    except Exception as e:
        print(f"[ERROR] Could not read movies data: {e}")
        return

    total_movies = len(df_movies)
    
    # 2. Skew Validation (Genre)
    print("\n--- 📊 GENRE DISTRIBUTION SKEW ---")
    genre_counts = df_movies['genre'].value_counts()
    print(genre_counts.to_string())
    
    drama_count = genre_counts.get("Drama", 0)
    drama_pct = (drama_count / total_movies) * 100
    print(f"\n[TARGET] 60.0% Drama")
    print(f"[RESULT] {drama_pct:.2f}% Drama ({drama_count}/{total_movies} rows)\n")

    # 3. Schema Evolution Validation
    print("--- 🧬 SCHEMA EVOLUTION (Missing 'country' column before target date) ---")
    
    # Split the dataset
    df_old = df_movies[df_movies['created_at'] < schema_change_date].copy()
    df_new = df_movies[df_movies['created_at'] >= schema_change_date].copy()

    print(f"Schema Change Date Target: {schema_change_date.strftime('%Y-%m-%d')}")
    
    # --- Safe Validation for PRE-SCHEMA Data ---
    print("\n[PRE-SCHEMA CHANGE] (Expect 100% Nulls in 'country')")
    print(f"Total Rows: {len(df_old)}")
    
    if 'country' in df_old.columns:
        null_count_old = df_old['country'].isna().sum()
    else:
        null_count_old = len(df_old)
        df_old['country'] = None 
        
    print(f"Null Count in 'country': {null_count_old}")
    print("Sample (Top 5 rows):")
    print(df_old[['movie_id', 'created_at', 'genre', 'country']].head(5).to_string(index=False))

    # --- Safe Validation for POST-SCHEMA Data ---
    print("\n[POST-SCHEMA CHANGE] (Expect 0% Nulls in 'country')")
    print(f"Total Rows: {len(df_new)}")
    
    if 'country' in df_new.columns:
        null_count_new = df_new['country'].isna().sum()
    else:
        null_count_new = len(df_new)
        df_new['country'] = "COLUMN COMPLETELY MISSING"
        
    print(f"Null Count in 'country': {null_count_new}")
    print("Sample (Top 5 rows):")
    print(df_new[['movie_id', 'created_at', 'genre', 'country']].head(5).to_string(index=False))

def validate_playbacks_data(offline_path: Path) -> None:
    """
    Validates the 'playbacks' dataset for bimodal completion rates and exact duplicates.
    
    Checks:
        1. Bimodal Distribution: Counts and prints playbacks with completion_rate <= 0.20 
           and >= 0.80. Target is ~80% of total rows.
        2. Exact Duplicates: Deduplicates based on strict composite keys and prints the 
           counts before/after to prove the ~5% duplication rate.
    """
    print("\n" + "="*70)
    print(" 📺 OFFLINE VALIDATION: PLAYBACKS LOGS")
    print("="*70)

    playbacks_path = offline_path / "playbacks"
    try:
        df_playbacks = pd.read_parquet(playbacks_path)
    except Exception as e:
        print(f"[ERROR] Could not read playbacks data: {e}")
        return

    total_playbacks = len(df_playbacks)

    # 1. Bimodal Distribution Validation
    print("\n--- 📈 BIMODAL WATCH DURATION DISTRIBUTION ---")
    low_watch_count = len(df_playbacks[df_playbacks['completion_rate'] <= 0.20])
    high_watch_count = len(df_playbacks[df_playbacks['completion_rate'] >= 0.80])
    total_bimodal = low_watch_count + high_watch_count
    bimodal_pct = (total_bimodal / total_playbacks) * 100

    print(f"Total Playbacks:           {total_playbacks:,}")
    print(f"Count (<= 20% Watched):    {low_watch_count:,}")
    print(f"Count (>= 80% Watched):    {high_watch_count:,}")
    print(f"[TARGET] ~80.0% fall into bimodal extremes")
    print(f"[RESULT] {bimodal_pct:.2f}% fall into bimodal extremes ({total_bimodal:,} rows)\n")

    # 2. Duplicates Validation
    print("--- 👯 DUPLICATE RECORDS CHECK ---")
    
    # Strict composite key check per instructions
    dedup_subset = ['user_id', 'movie_id', 'click_ts']
    
    # Graceful fallback in case generated columns are slightly different in actual Parquet
    available_subset = [col for col in dedup_subset if col in df_playbacks.columns]
    
    rows_before = len(df_playbacks)
    df_dedup = df_playbacks.drop_duplicates(subset=available_subset)
    rows_after = len(df_dedup)
    dup_pct = ((rows_before - rows_after) / rows_before) * 100

    print(f"Deduplication Keys Used:   {available_subset}")
    print(f"Rows Before Deduplication: {rows_before:,}")
    print(f"Rows After Deduplication:  {rows_after:,}")
    print(f"[TARGET] ~5.00% duplicates")
    print(f"[RESULT] {dup_pct:.2f}% duplicates removed\n")

    # 3. High Cardinality Validation
    print("--- 🔀 HIGH CARDINALITY ANALYSIS ---")
    unique_users = df_playbacks['user_id'].nunique() if 'user_id' in df_playbacks.columns else 0
    unique_movies = df_playbacks['movie_id'].nunique() if 'movie_id' in df_playbacks.columns else 0
    unique_playbacks = df_playbacks['playback_id'].nunique() if 'playback_id' in df_playbacks.columns else 0
    
    # Calculate unique combinations across all three specified structural dimensions
    cardinality_subset = ['user_id', 'movie_id', 'playback_id']
    available_card_subset = [col for col in cardinality_subset if col in df_playbacks.columns]
    unique_combinations = len(df_playbacks.drop_duplicates(subset=available_card_subset))

    print(f"Distinct User IDs:         {unique_users:,}")
    print(f"Distinct Movie IDs:        {unique_movies:,}")
    print(f"Distinct Playback IDs:     {unique_playbacks:,}")
    print(f"[RESULT] Total Unique [User x Movie x Playback] Combinations: {unique_combinations:,}\n")


def validate_streaming_events(streaming_path: Path) -> None:
    """
    Validates the 'streaming_events.jsonl' dataset for traffic spikes and late arrivals.
    
    Checks:
        1. Burst Traffic: Groups events by minute, finding the minimum and maximum traffic.
           Validates the timestamp of the max traffic spike.
        2. Late Arrivals: Compares ingestion timestamp (created_ts) vs actual event timestamp 
           (event_ts) to calculate the percentage of delayed packets.
    """
    print("\n" + "="*70)
    print(" ⚡ STREAMING VALIDATION: CLICKSTREAM EVENTS")
    print("="*70)

    events_path = streaming_path / "streaming_events.jsonl"
    try:
        df_events = pd.read_json(events_path, lines=True)
    except Exception as e:
        print(f"[ERROR] Could not read streaming data: {e}")
        return

    # 1. Burst Validation
    print("\n--- 🌊 TRAFFIC BURST ANALYSIS ---")
    df_events['event_ts_dt'] = pd.to_datetime(df_events['event_ts'])
    
    # Drop duplicates based on the actual event payload to find pure baseline traffic
    df_dedup_events = df_events.drop_duplicates(subset=['event_id'])
    
    # Group by minute to count events per minute on the CLEANED data
    events_per_minute = df_dedup_events.groupby(df_dedup_events['event_ts_dt'].dt.floor('Min')).size()
    
    # Use Median instead of Mean/Min
    median_events = events_per_minute.median()
    
    # Calculate Max on the RAW data (because bursts realistically contain duplicates in a stream)
    raw_events_per_minute = df_events.groupby(df_events['event_ts_dt'].dt.floor('Min')).size()
    max_events = raw_events_per_minute.max()
    max_events_time = raw_events_per_minute.idxmax()

    print(f"Configured Burst Windows:  ['12:00-12:20', '20:00-20:20']")
    print(f"Median Events / Minute:    {median_events:,.0f} (Baseline)")
    print(f"Maximum Events / Minute:   {max_events:,} (Spike)")
    print(f"[RESULT] Max Spike Occurred at: {max_events_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 2. Late Arrival Validation
    print("--- 🐢 LATE ARRIVAL (OUT-OF-ORDER) DATA ---")
    df_events['created_ts_dt'] = pd.to_datetime(df_events['created_ts'], format='ISO8601')
    
    # Calculate time difference in seconds
    df_events['delay_seconds'] = (df_events['created_ts_dt'] - df_events['event_ts_dt']).dt.total_seconds()
    
    # Count rows where created_ts is explicitly later than event_ts (e.g., > 1 second late)
    late_events_count = len(df_events[df_events['delay_seconds'] >= 1.0])
    total_events = len(df_events)
    late_pct = (late_events_count / total_events) * 100

    print(f"Total Streaming Events:    {total_events:,}")
    print(f"Late Arriving Events:      {late_events_count:,}")
    print(f"[TARGET] ~12.0%")
    print(f"[RESULT] {late_pct:.2f}% of events are arriving late.\n")

    # 3. Duplicates Validation
    print("--- 👯 DUPLICATE RECORDS CHECK (Network Retries) ---")
    rows_before = len(df_events)
    unique_events = df_events['event_id'].nunique()
    duplicates_found = rows_before - unique_events
    
    # Calculate percentage against the base unique events
    dup_pct = (duplicates_found / unique_events) * 100

    print(f"Total Streaming Events:    {rows_before:,}")
    print(f"Unique Event IDs:          {unique_events:,}")
    print(f"Duplicate IDs Found:       {duplicates_found:,}")
    print(f"[TARGET] ~2.00% duplicates")
    print(f"[RESULT] {dup_pct:.2f}% duplicate events found.\n")


def validate_data_gen(
    offline_data_path: Path, 
    streaming_data_path: Path, 
    schema_change_date: pd.Timestamp
) -> None:
    """
    Master orchestration function for running the full validation suite.
    """
    print("\n" + "#"*70)
    print("             DATA GENERATION QUALITY REPORT CARD")
    print("#"*70)

    validate_movies_data(offline_path=offline_data_path, schema_change_date=schema_change_date)
    validate_playbacks_data(offline_path=offline_data_path)
    validate_streaming_events(streaming_path=streaming_data_path)

    print("\n" + "#"*70)
    print("                    END OF VALIDATION REPORT")
    print("#"*70 + "\n")


if __name__ == "__main__":
    
    # Defined Schema change date from the original config
    SCHEMA_CHANGE_DATE = pd.Timestamp("2026-04-07")

    # Run the validation suite directly using the centralized paths
    validate_data_gen(
        offline_data_path=OFFLINE_DATA_PATH,
        streaming_data_path=STREAMING_DATA_PATH,
        schema_change_date=SCHEMA_CHANGE_DATE
    )