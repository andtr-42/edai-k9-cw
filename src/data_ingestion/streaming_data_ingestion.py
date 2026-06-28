import os
import json
import time
from datetime import datetime
from pathlib import Path
from confluent_kafka import Producer
from src.config import STREAMING_DATA_PATH

# Configuration Kafka Constants
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "streaming_movie_events"
STREAMING_EVENT_PATH = os.path.join(STREAMING_DATA_PATH, "streaming_events.jsonl")

# Traffic Rates (Events per minute)
BASELINE_RATE = 100
BURST_RATE = 3000


def is_burst_period(event_ts_str: str) -> bool:
    """Checks if the event's timestamp falls within the 20-minute burst windows
    (12:00 - 12:20 or 20:00 - 20:20).
    """
    # Parse the ISO timestamp from the event data
    dt = datetime.fromisoformat(event_ts_str)
    current_hour = dt.hour
    current_minute = dt.minute

    # Check for 12:00 - 12:20 window
    if current_hour == 12 and (0 <= current_minute < 20):
        return True
    # Check for 20:00 - 20:20 window
    if current_hour == 20 and (0 <= current_minute < 20):
        return True

    return False


def load_events_to_memory(file_path: str) -> list:
    """Loads and pre-parses the JSONL file into an in-memory list to prevent
    disk I/O bottlenecks during high-throughput bursts.
    """
    print(f"[*] Loading data into memory from: {file_path}...")
    events = []
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found at {file_path}. Please verify the path.")

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    print(f"[+] Successfully loaded {len(events)} events into memory.")
    return events


def main():
    # Initialize Kafka Producer
    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 10,
        "compression.type": "snappy",
        "acks": 1,
    }
    producer = Producer(producer_config)

    # Load file to RAM
    try:
        event_cache = load_events_to_memory(STREAMING_EVENT_PATH)
    except Exception as e:
        print(f"[-] Critical Error loading file: {e}")
        return

    if not event_cache:
        print("[-] Cache is empty. Exiting.")
        return

    print("[*] Starting continuous streaming simulation. Press Ctrl+C to stop.")
    event_index = 0
    total_events = len(event_cache)

    try:
        for event_index, event in enumerate(event_cache):
            
            # 2. Determine Rate based on the event's actual timestamp
            if is_burst_period(event["event_ts"]):
                current_rate = BURST_RATE
                state_label = "BURST"
            else:
                current_rate = BASELINE_RATE
                state_label = "BASELINE"

            delay = 60.0 / current_rate

            # 3. Produce asynchronously to Kafka (Data remains untouched)
            payload = json.dumps(event).encode("utf-8")
            producer.produce(TOPIC_NAME, value=payload)

            # Serve background delivery callbacks
            producer.poll(0)

            # Optional verbose logging
            if event_index % 100 == 0 or state_label == "BURST":
                print(
                    f"[{state_label}] Pacing at {current_rate} ev/min. Stream Index: {event_index}",
                    flush=True,
                )

            # 4. Throttle
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n[-] Graceful shutdown triggered by user.")
    finally:
        print("[*] Flushing outstanding Kafka messages...")
        producer.flush(timeout=10)
        print("[+] Producer safely stopped.")


if __name__ == "__main__":
    main()