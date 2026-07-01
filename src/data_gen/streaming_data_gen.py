"""
Module: src.generation.streaming_data_gen
Type: Component (Data Generation / Simulation Pipeline)

Description:
    Generates reproducible, high-fidelity synthetic streaming event data for the
    streaming data platform. Simulates real-time clickstream and video tracking behavior,
    injecting complex production stream characteristics like spike traffic, out-of-order 
    data, and duplicate message delivery. Designed to simulate input for a downstream 
    real-time message broker or raw staging bucket.

Data Models Generated:
    1. Streaming Events (streaming_events.jsonl): Chronologically sorted JSON Lines log 
       containing atomic clickstream actions (impress, view, click) and transactional 
       playback tracking state changes (start, heartbeat, pause, resume, etc.).

Engine Features & Guardrails:
    - Ingestion vs Event Time Drift: Simulates late-arriving data by creating an offset 
      between event timestamps (`event_ts`) and system processing timestamps (`created_ts`).
    - Network Failure Simulation: Injects systemic duplicate records with isolated forward-shifted 
      timestamps to mimic stream delivery retries.
    - Dynamic Traffic Bursts: Replicates organic daily high-traffic windows via configuration-driven 
      multipliers applied to base message generation limits.
    - Contextual Sessionization: Factors time-blocked sequences per user profile to generate 
      deterministic user session progression.

Runtime Context:
    Must be executed from the project root repository directory as a Python module:
    $ python -m src.generation.streaming_data_gen
"""

import json
import random
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from src.config import OFFLINE_DATA_PATH, STREAMING_DATA_PATH
from pathlib import Path

RANDOM_SEED = 42
MINUTES_PER_HOUR = 60

events_config = {
    "event_types": [
        "impress",
        "view",
        "click",
        "start",
        "heartbeat",
        "pause",
        "resume",
        "fast-forward",
        "stop",
        "complete",
    ],
}

def save_events_to_jsonl(df_events: pd.DataFrame, output_dir: "Path") -> None:
    """
    Writes a pandas DataFrame of events to a JSON Lines (.jsonl) file.
    """    

    # Convert DataFrame to a list of dicts. 
    # Replace np.nan with None so json.dumps outputs valid 'null' instead of 'NaN'.
    events_list = df_events.replace({np.nan: None}).to_dict(orient="records")

    with open(output_dir, "w") as f:
        for event in events_list:
            f.write(json.dumps(event) + "\n")
            
    print(f"Generated {len(df_events)} streaming events and saved to {output_dir}")

def streaming_data_generator(
    offline_data_path: "Path",
    base_date: pd.Timestamp,
    hours_history: int,
    base_events_per_min: int,
    burst_multiplier: int,
    burst_windows: list,
    late_arrival_rate: float,
    late_delay_min_max: list,
    duplicate_rate: float,
    duplicate_delay_min_max: list,
) -> pd.DataFrame:

    print("Starting to generate streaming events using unified loop...")

    # Read df_users and df_movies
    df_users = pd.read_parquet(offline_data_path / "users")
    df_movies = pd.read_parquet(offline_data_path / "movies.parquet")

    # Extract clean lists and dicts from DataFrames for fast random sampling
    user_ids = df_users["user_id"].tolist()
    movie_runtimes = df_movies.set_index("movie_id")["runtime_seconds"].to_dict()
    movie_ids = list(movie_runtimes.keys())

    # Need to calculate the next date because it gonna generates the event backward
    next_day = base_date + timedelta(days=1)
    
    events = []
    playback_counter = 0  # Auto-incremental counter starting from 0
    
    # Loop through each minute in the requested history
    for minute_offset in range(hours_history * 60):
        event_ts = next_day - timedelta(minutes=minute_offset)
        hour_minute_str = event_ts.strftime("%H:%M")

        # Determine if we are in a burst window
        in_burst = any(
            start <= hour_minute_str < end
            for start, end in [window.split("-") for window in burst_windows]
        )

        # Set the event target once to avoid duplicating the entire generation block
        events_per_min = (base_events_per_min * burst_multiplier) if in_burst else base_events_per_min

        # Generate all events for this specific minute
        for i in range(events_per_min):
            event_id = f"event_{minute_offset}_{i}"
            user_id = random.choice(user_ids)
            event_type = random.choice(events_config["event_types"])

            random_second = random.randint(0, 59)
            exact_event_ts = event_ts + timedelta(seconds=random_second)
            
            # Implement time-block session distribution
            # 00:00-07:59 -> Block 1 | 08:00-15:59 -> Block 2 | 16:00-23:59 -> Block 3
            time_block = exact_event_ts.hour // 8
            session_id = f"raw_group_{user_id}_{exact_event_ts.date()}_{time_block}"

            # Calculate ingestion time (created_ts) vs actual event time (event_ts)
            is_late_arrival = random.random() < late_arrival_rate
            if is_late_arrival:
                late_delay_sec = random.uniform(*late_delay_min_max)
                created_ts = exact_event_ts + timedelta(seconds=late_delay_sec)
            else:
                created_ts = exact_event_ts

            # Implement current_playback_offset_seconds logic
            if event_type in ["impress"]:
                movie_id = None
                playback_id = None
                playback_start_ts = None
                current_playback_offset_seconds = None

            elif event_type in ["view"]:
                movie_id = random.choice(movie_ids)
                playback_id = None
                playback_start_ts = None
                current_playback_offset_seconds = None

            elif event_type == "click":
                movie_id = random.choice(movie_ids)

                # Auto-incremental playback ID
                playback_id = f"playback_{playback_counter}"
                playback_counter += 1
                playback_start_ts = None
                current_playback_offset_seconds = None
            else:
                # Video events require valid playback attributes
                movie_id = random.choice(movie_ids)

                # Auto-incremental playback ID
                playback_id = f"playback_{playback_counter}"
                playback_counter += 1
                
                # 1 to 5 seconds buffering/loading delay after the event is triggered
                playback_start_ts = (event_ts + timedelta(seconds=random.randint(1, 5))).isoformat()
                
                runtime_seconds = movie_runtimes.get(movie_id, 7200)

                if event_type == "complete":
                    current_playback_offset_seconds = runtime_seconds
                else:
                    current_playback_offset_seconds = random.randint(0, runtime_seconds)

            events.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "event_ts": exact_event_ts.isoformat(),
                    "created_ts": created_ts.isoformat(),
                    "user_id": user_id,
                    "session_id": session_id,
                    "movie_id": movie_id,
                    "playback_id": playback_id,
                    "playback_start_ts": playback_start_ts,
                    "current_playback_offset_seconds": current_playback_offset_seconds,
                }
            )

        # Print progress every 60 minutes (1 hour) to reduce terminal spam
        if minute_offset % 60 == 0:
            print(f"Generated events for minute offset {minute_offset} ({event_ts.strftime('%Y-%m-%d %H:%M:%S')}). Total: {len(events)}")

    # Generate Duplicate Events
    n_duplicates = int(len(events) * duplicate_rate)
    duplicate_events_sample = random.sample(events, n_duplicates)

    for original_event in duplicate_events_sample:
        # Must use .copy() otherwise you mutate the original event in the list
        dup_event = original_event.copy() 
        
        # 1. Calculate the delay strictly once for this duplicate
        delay_seconds = random.randint(duplicate_delay_min_max[0], duplicate_delay_min_max[1])
        delay_delta = timedelta(seconds=delay_seconds)
        
        # 2. Shift the event_ts forward
        original_event_ts = datetime.fromisoformat(dup_event["event_ts"])
        dup_event["event_ts"] = (original_event_ts + delay_delta).isoformat()
        
        # 3. Shift the created_ts forward by the exact same amount
        original_created_ts = datetime.fromisoformat(dup_event["created_ts"])
        dup_event["created_ts"] = (original_created_ts + delay_delta).isoformat()

        events.append(dup_event)

    # Sort events chronologically to simulate a real stream
    events.sort(key=lambda x: x["event_ts"])
    
    # Convert list of dictionaries into a pandas DataFrame
    df_events = pd.DataFrame(events)

    # make sure user a will have session 1 before having session 2
    df_events["session_sequence"] = df_events.groupby("user_id")["session_id"].transform(lambda x: pd.factorize(x)[0] + 1)
    
    # Build the final, clean session_id string
    df_events["session_id"] = "user_" + df_events["user_id"].astype(str) + "_sess_" + df_events["session_sequence"].astype(str)
    
    # Drop the temporary sequence column
    df_events.drop(columns=["session_sequence"], inplace=True)

    print("\n Streaming event data: ")
    print(df_events.head().to_string())
    print(f"\nGenerated {len(df_events)} streaming events.")
    
    return df_events
    
# if __name__ == "__main__":
    
#     # Setup the environments
#     np.random.seed(RANDOM_SEED)
#     STREAMING_DATA_PATH.mkdir(parents=True, exist_ok=True)

#     # Execution parameters 
#     # generate data backward, so that it will generate data for the 2026-06-08
#     base_date = pd.Timestamp("2026-06-09")  # Set static date for reproducible, one day ahead of the date inside the offline data
#     hours_history = 24
#     base_events_per_min = 100
#     burst_multiplier = 30
#     burst_windows = ["12:00-12:20", "20:00-20:20"]
#     late_arrival_rate = 0.12
#     late_delay_min_max = [5, 45]
#     duplicate_rate = 0.02
#     duplicate_delay_min_max = [60, 180]

#     df_events = generate_streaming_events(
#         offline_data_path=str(OFFLINE_DATA_PATH),
#         base_date=base_date,
#         hours_history=hours_history,
#         base_events_per_min=base_events_per_min,
#         burst_multiplier=burst_multiplier,
#         burst_windows=burst_windows,
#         late_arrival_rate=late_arrival_rate,
#         late_delay_min_max=late_delay_min_max,
#         duplicate_rate=duplicate_rate,
#         duplicate_delay_min_max = duplicate_delay_min_max,
#     )

#     # Change from: output_file = os.path.join(STREAMING_DATA_DIR, f"streaming_events.jsonl")
#     output_file = STREAMING_DATA_PATH / "streaming_events.jsonl"

#     save_events_to_jsonl(df_events, str(output_file))
