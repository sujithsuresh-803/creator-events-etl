"""
Generate a realistic-ish raw event dataset for the ETL pipeline.

Produces:
  data/raw/events.jsonl   — semi-structured engagement events (the "bronze" layer)
  data/dim/artists.csv    — an artist dimension table to join against

The raw events are intentionally messy (duplicates, nulls, bad rows) so the
ETL job has real cleaning to do.

Usage:
    python src/generate_data.py --events 200000 --artists 500
"""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

ARCHETYPES = ["reels", "acoustic", "instrumental", "edm", "composer", "cover"]
PLATFORMS = ["tiktok", "instagram", "youtube", "spotify", "soundcloud"]
EVENT_TYPES = ["stream", "save", "follow", "share", "goal_progress"]
COUNTRIES = ["US", "IN", "GB", "BR", "DE", "PH", "NG", "ID", "MX", "CA"]


def make_artists(n):
    rows = []
    for i in range(n):
        rows.append({
            "artist_id": f"a_{i:05d}",
            "artist_name": f"Artist {i:05d}",
            "archetype": random.choice(ARCHETYPES),
            "signup_country": random.choice(COUNTRIES),
        })
    return rows


def make_events(n, artist_ids):
    start = datetime(2026, 1, 1)
    for i in range(n):
        ts = start + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 30))
        etype = random.choices(EVENT_TYPES, weights=[50, 20, 10, 10, 10])[0]
        ev = {
            "event_id": f"e_{i:08d}",
            "user_id": f"u_{random.randint(0, 80000):06d}",
            "artist_id": random.choice(artist_ids),
            "event_type": etype,
            "platform": random.choice(PLATFORMS),
            "country": random.choice(COUNTRIES),
            "ts": ts.isoformat(),
            # goal_progress carries a numeric value; others usually don't
            "value": random.randint(1, 5) if etype == "goal_progress" else None,
        }
        # Inject messiness the ETL must handle:
        if random.random() < 0.02:      # ~2% missing artist_id
            ev["artist_id"] = None
        if random.random() < 0.01:      # ~1% bad timestamp
            ev["ts"] = "not-a-date"
        yield ev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--artists", type=int, default=500)
    ap.add_argument("--out", default="data")
    ap.add_argument("--dup-rate", type=float, default=0.03, help="fraction of duplicated events")
    args = ap.parse_args()

    os.makedirs(os.path.join(args.out, "raw"), exist_ok=True)
    os.makedirs(os.path.join(args.out, "dim"), exist_ok=True)

    artists = make_artists(args.artists)
    artist_ids = [a["artist_id"] for a in artists]

    dim_path = os.path.join(args.out, "dim", "artists.csv")
    with open(dim_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["artist_id", "artist_name", "archetype", "signup_country"])
        w.writeheader()
        w.writerows(artists)

    raw_path = os.path.join(args.out, "raw", "events.jsonl")
    written = 0
    with open(raw_path, "w", encoding="utf-8") as f:
        for ev in make_events(args.events, artist_ids):
            f.write(json.dumps(ev) + "\n")
            written += 1
            # Duplicate some events (same event_id) to test dedup.
            if random.random() < args.dup_rate:
                f.write(json.dumps(ev) + "\n")
                written += 1

    print(f"Wrote {written:,} raw event lines -> {raw_path}")
    print(f"Wrote {len(artists):,} artists -> {dim_path}")


if __name__ == "__main__":
    main()
