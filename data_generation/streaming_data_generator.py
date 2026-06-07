import argparse
import json
import random
import time
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from offline_data_generator import OUTPUT_DIR_OFFLINE, N_USERS, N_MOVIES

OUTPUT_DIR_STREAMING = "../data/raw/streaming/"
os.makedirs(OUTPUT_DIR_STREAMING, exist_ok=True)
np.random.seed(42)

EVENT_TYPES = [
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
]
HOURS_HISTORY = 24  # generate events from the last 24 hours
LATE_DELAY_MIN_SEC = 30  # event_timestamp is at least 30 s in the past
LATE_DELAY_MAX_SEC = 300  # … up to 5 minutes in the past

BASELINE_EVENT_RATE_PER_MINUTE = 100  # baseline rate of events per minute
BURST_EVENT_RATE_PER_MINUTE = 3000  # event rate during bursts
BURST_START_HOUR_1 = 12  # first burst starts at 12:00
BURST_START_HOUR_2 = 20  # second burst starts at 18:00
BURST_DURATION_MINUTES = 20  # each burst lasts for 20 minutes

# def generate_one_streaming_event(event_id: int, event_ts: datetime, created_ts: datetime)


def generate_streaming_events(
    hours_history: int,
    base_events_per_min: int,
    burst_multipler: int,
    burst_windows: list,  # ["12:00-12:20", "20:00-20:20"]
    late_arrival_rate: float,
    late_delay_min_max: tuple,  # [5, 45]
    duplicate_rate_stream: float,
) -> None:
    
    print("Starting to generate streaming events...")

    # read the movie data to get the actual runtime of each movie so that we can generate realistic playback events
    df_movies = pd.read_parquet(
        os.path.join(OUTPUT_DIR_OFFLINE, "movies_0.6_drama_20260304")
    )
    movie_runtimes = df_movies.set_index("movie_id")["runtime_seconds"].to_dict()

    # loop through each minute in the last 24 hours and generate events
    events = []
    now = datetime.now(timezone.utc)
    for minute_offset in range(hours_history * 60):
        event_ts = now - timedelta(minutes=minute_offset)

        # determine if we are in a burst window
        hour_minute_str = event_ts.strftime("%H:%M")
        in_burst = any(
            start <= hour_minute_str < end
            for start, end in [window.split("-") for window in burst_windows]
        )

        # randomly if the event is a late arrival which makes the created_ts different from the event_ts
        is_late_arrival = random.random() < late_arrival_rate

        if in_burst:
            events_per_min = base_events_per_min * burst_multipler
            delay = 60 / events_per_min  # delay between events in seconds

            for i in range(events_per_min):
                event_id = f"event_{minute_offset}_{i}"
                user_id = random.randint(1, N_USERS)
                event_type = random.choice(EVENT_TYPES)
                session_id = f"session_{random.randint(1, 100000)}"

                if is_late_arrival:
                    late_delay_sec = random.uniform(*late_delay_min_max)
                    created_ts = event_ts + timedelta(seconds=late_delay_sec)
                else:
                    created_ts = event_ts

                if event_type in ["impress", "view", "click"]:
                    # leave the movie_id, playback_id, playback_start_ts, duration_watch_seconds as null for impression and view events
                    movie_id = None
                    playback_id = None
                    playback_start_ts = None
                    duration_watch_seconds = None

                elif event_type in ["complete"]:
                    # for complete events, we need to make sure the duration_watch_seconds is equal to the runtime of the movie to make it realistic
                    movie_id = random.randint(1, N_MOVIES)
                    playback_id = f"playback_{random.randint(1, 100000)}"
                    playback_start_ts = event_ts.isoformat()
                    runtime_seconds = movie_runtimes.get(
                        movie_id, 7200
                    )  # default to 2 hours if not found
                    duration_watch_seconds = runtime_seconds

                else:
                    movie_id = random.randint(1, N_MOVIES)
                    playback_id = f"playback_{random.randint(1, 100000)}"
                    playback_start_ts = event_ts.isoformat()
                    runtime_seconds = movie_runtimes.get(
                        movie_id, 7200
                    )  # default to 2 hours if not found
                    duration_watch_seconds = random.randint(0, runtime_seconds)

                events.append(
                    {
                        "event_id": event_id,
                        "event_type": event_type,
                        "event_ts": event_ts.isoformat(),
                        "created_ts": created_ts.isoformat(),
                        "user_id": user_id,
                        "session_id": session_id,
                        "movie_id": movie_id,
                        "playback_id": playback_id,
                        "playback_start_ts": playback_start_ts,
                        "duration_watch_seconds": duration_watch_seconds,
                    }
                )

                # time.sleep(delay)  # simulate real-time event generation

        else:
            # generate baseline events at a lower rate
            events_per_min = base_events_per_min
            delay = 60 / events_per_min  # delay between events in seconds

            for i in range(events_per_min):
                event_id = f"event_{minute_offset}_{i}"
                user_id = random.randint(1, N_USERS)
                event_type = random.choice(EVENT_TYPES)
                session_id = f"session_{random.randint(1, 100000)}"

                if is_late_arrival:
                    late_delay_sec = random.uniform(*late_delay_min_max)
                    created_ts = event_ts + timedelta(seconds=late_delay_sec)
                else:
                    created_ts = event_ts

                if event_type in ["impress", "view", "click"]:
                    # leave the movie_id, playback_id, playback_start_ts, duration_watch_seconds as null for impression and view events
                    movie_id = None
                    playback_id = None
                    playback_start_ts = None
                    duration_watch_seconds = None

                elif event_type in ["complete"]:
                    # for complete events, we need to make sure the duration_watch_seconds is equal to the runtime of the movie to make it realistic
                    movie_id = random.randint(1, N_MOVIES)
                    playback_id = f"playback_{random.randint(1, 100000)}"
                    playback_start_ts = event_ts.isoformat()
                    runtime_seconds = movie_runtimes.get(
                        movie_id, 7200
                    )  # default to 2 hours if not found
                    duration_watch_seconds = runtime_seconds

                else:
                    movie_id = random.randint(1, N_MOVIES)
                    playback_id = f"playback_{random.randint(1, 100000)}"
                    playback_start_ts = event_ts.isoformat()
                    runtime_seconds = movie_runtimes.get(
                        movie_id, 7200
                    )  # default to 2 hours if not found
                    duration_watch_seconds = random.randint(0, runtime_seconds)

                events.append(
                    {
                        "event_id": event_id,
                        "event_type": event_type,
                        "event_ts": event_ts.isoformat(),
                        "created_ts": created_ts.isoformat(),
                        "user_id": user_id,
                        "session_id": session_id,
                        "movie_id": movie_id,
                        "playback_id": playback_id,
                        "playback_start_ts": playback_start_ts,
                        "duration_watch_seconds": duration_watch_seconds,
                    }
                )

                # time.sleep(delay)  # simulate real-time event generation        
        print(f"Generated events for minute offset {minute_offset} ({event_ts.strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"Total events generated so far: {len(events)} \n")

    # add some duplicates to simulate duplicate events in the stream
    n_duplicates = int(len(events) * (duplicate_rate_stream / 100))
    duplicate_events = random.sample(events, n_duplicates)

    # must search for 1 and 3 minutes after the original event and find the right place to insert the duplicate event to make it realistic
    for dup_event in duplicate_events:
        original_event_ts = datetime.fromisoformat(dup_event["event_ts"])
        duplicate_event_ts = original_event_ts + timedelta(
            seconds=random.randint(60, 180) # duplicate event occurs 1-3 minutes after the original event
        )  # duplicate event occurs 1-3 minutes after the original event
        dup_event["event_id"] = f"{dup_event['event_id']}_dup"
        dup_event["event_ts"] = duplicate_event_ts.isoformat()
        events.append(dup_event)

    # sort events by event_ts to simulate the order they would arrive in the stream
    events.sort(key=lambda x: x["event_ts"])
    # write events to a jsonl file
    output_file = os.path.join(
        OUTPUT_DIR_STREAMING,
        f"streaming_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
    )
    with open(output_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"Generated {len(events)} streaming events and saved to {output_file}")


def main():
    hours_history = HOURS_HISTORY
    base_events_per_min = BASELINE_EVENT_RATE_PER_MINUTE
    burst_multiplier = BURST_EVENT_RATE_PER_MINUTE // BASELINE_EVENT_RATE_PER_MINUTE
    burst_windows = [
        f"{BURST_START_HOUR_1:02d}:00-{(BURST_START_HOUR_1 + BURST_DURATION_MINUTES // 60) % 24:02d}:{BURST_DURATION_MINUTES % 60:02d}",
        f"{BURST_START_HOUR_2:02d}:00-{(BURST_START_HOUR_2 + BURST_DURATION_MINUTES // 60) % 24:02d}:{BURST_DURATION_MINUTES % 60:02d}",
    ]
    late_arrival_rate = 0.1  # 10% of events are late
    late_delay_min_max = (LATE_DELAY_MIN_SEC, LATE_DELAY_MAX_SEC)
    duplicate_rate_stream = 1.5  # 1.5% of events are duplicates

    generate_streaming_events(
        hours_history,
        base_events_per_min,
        burst_multiplier,
        burst_windows,
        late_arrival_rate,
        late_delay_min_max,
        duplicate_rate_stream,
    )


if __name__ == "__main__":
    main()
