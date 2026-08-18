"""
Export a FastF1 session into the per-driver CSVs the Java telemetry
producer (DriverCSVStream.java) expects for live-streaming simulation.

This is for the STREAMING dataset only — one race's worth of per-driver
telemetry, replayed through Kafka to simulate a live session. It is not
the historical training corpus used by train_all_models.py.

Usage:
    pip install fastf1 pandas
    python export_fastf1_csv.py --year 2026 --event "Miami Grand Prix" --session R

Output:
    <output-dir>/<year>_<Event_Name_With_Underscores>_<session>_<driver_code>.csv
    e.g. raw_telemetry_per_driver/2026_Miami_Grand_Prix_R_VER.csv

Required columns for the Java parser (will reject the file without these):
    lapnumber, time_ms, speed, driver, event, year, session

Additional columns the rest of the pipeline (feature engineering /
inference) expects, all populated here:
    RPM, nGear, Throttle, Brake, DRS, Distance, RelativeDistance,
    X, Y, Z, Stint, Compound, is_pit_lap, TrackTemp, AirTemp, Rainfall,
    weather, corner_id, track_segment, hard_brake, full_throttle,
    driver_name, driver_code, team

NOTE: this script has not been executed in this environment (no network
access to FastF1's data sources here) — it's written directly against
FastF1's documented API and the exact column list pulled from
TelemetryRecord.java / DriverCSVStream.java in this repo. Run it locally
and sanity-check a row or two against those two files before trusting it
for a real stream.
"""

import argparse
import os
import numpy as np
import pandas as pd
import fastf1

# Corner-proximity threshold (meters) for tagging a telemetry sample as
# "in" a corner vs. on a straight. Tune per circuit if labels look off.
CORNER_PROXIMITY_M = 40.0

# A sample counts as "hard braking" if Brake is applied and speed is
# dropping faster than this (km/h per sample). FastF1 car data is ~10Hz,
# so this is roughly a hard-braking threshold, not a physics constant —
# adjust if your circuit/sample-rate combination needs it.
HARD_BRAKE_DROP_KMH = 8.0

FULL_THROTTLE_PCT = 99.0


def build_corner_lookup(session):
    """Returns a sorted array of corner distances-around-lap and their numbers,
    from FastF1's circuit info, for nearest-corner tagging."""
    circuit_info = session.get_circuit_info()
    corners = circuit_info.corners[["Distance", "Number"]].sort_values("Distance")
    return corners["Distance"].to_numpy(), corners["Number"].to_numpy()


def tag_corner(distance_around_lap, corner_distances, corner_numbers):
    if len(corner_distances) == 0 or pd.isna(distance_around_lap):
        return None, "Straight"
    idx = np.searchsorted(corner_distances, distance_around_lap)
    candidates = [i for i in (idx - 1, idx) if 0 <= i < len(corner_distances)]
    if not candidates:
        return None, "Straight"
    nearest = min(candidates, key=lambda i: abs(corner_distances[i] - distance_around_lap))
    if abs(corner_distances[nearest] - distance_around_lap) <= CORNER_PROXIMITY_M:
        n = int(corner_numbers[nearest])
        return n, f"Corner {n}"
    return None, "Straight"


def export_driver(session, drv, event_display, event_slug, year, session_code,
                   corner_distances, corner_numbers, weather):
    laps = session.laps.pick_drivers(drv)
    if laps.empty:
        print(f"  [skip] no laps for driver {drv}")
        return None

    driver_name = laps["Driver"].iloc[0] if "Driver" in laps.columns else drv
    driver_code = laps["Driver"].iloc[0] if "Driver" in laps.columns else drv
    team = laps["Team"].iloc[0] if "Team" in laps.columns else ""

    per_lap_frames = []
    for _, lap in laps.iterlaps():
        try:
            tel = lap.get_telemetry()
        except Exception as e:
            print(f"  [warn] driver {drv} lap {lap.get('LapNumber')}: telemetry unavailable ({e})")
            continue
        if tel is None or tel.empty:
            continue

        tel = tel.copy()
        tel["LapNumber"] = int(lap["LapNumber"]) if pd.notna(lap["LapNumber"]) else None
        tel["Stint"] = int(lap["Stint"]) if pd.notna(lap.get("Stint")) else None
        tel["Compound"] = lap.get("Compound", "")
        tel["is_pit_lap"] = int(pd.notna(lap.get("PitInTime")) or pd.notna(lap.get("PitOutTime")))

        per_lap_frames.append(tel)

    if not per_lap_frames:
        print(f"  [skip] no usable telemetry for driver {drv}")
        return None

    df = pd.concat(per_lap_frames, ignore_index=True)

    # time_ms: FastF1's SessionTime is a timedelta from session start.
    df["time_ms"] = (df["SessionTime"].dt.total_seconds() * 1000).astype("int64")

    # Merge weather by nearest timestamp (weather is sampled much less
    # frequently than car telemetry).
    if weather is not None and not weather.empty:
        df = pd.merge_asof(
            df.sort_values("Time"),
            weather.sort_values("Time"),
            on="Time",
            direction="nearest",
        )
    else:
        for col in ("AirTemp", "TrackTemp", "Rainfall"):
            df[col] = None
        df["weather"] = ""

    if "Rainfall" in df.columns:
        df["weather"] = df["Rainfall"].apply(lambda r: "Wet" if r else "Dry")

    # RelativeDistance: FastF1 provides this via add_relative_distance();
    # fall back to normalizing Distance if it's missing on some laps.
    if "RelativeDistance" not in df.columns:
        max_d = df["Distance"].max() or 1.0
        df["RelativeDistance"] = df["Distance"] / max_d

    corner_tags = df["Distance"].apply(lambda d: tag_corner(d, corner_distances, corner_numbers))
    df["corner_id"] = corner_tags.apply(lambda t: t[0])
    df["track_segment"] = corner_tags.apply(lambda t: t[1])

    speed_delta = df["Speed"].diff().fillna(0)
    df["hard_brake"] = ((df.get("Brake", 0).astype(bool)) & (speed_delta <= -HARD_BRAKE_DROP_KMH)).astype(int)
    df["full_throttle"] = (df["Throttle"] >= FULL_THROTTLE_PCT).astype(int)

    df["driver"] = drv
    df["driver_name"] = driver_name
    df["driver_code"] = driver_code
    df["team"] = team
    df["event"] = event_display
    df["year"] = year
    df["session"] = session_code

    out = pd.DataFrame({
        "lapnumber": df["LapNumber"],
        "time_ms": df["time_ms"],
        "speed": df["Speed"],
        "RPM": df.get("RPM"),
        "nGear": df.get("nGear"),
        "Throttle": df.get("Throttle"),
        "Brake": df.get("Brake", 0).astype(int),
        "DRS": df.get("DRS"),
        "Distance": df.get("Distance"),
        "RelativeDistance": df.get("RelativeDistance"),
        "X": df.get("X"),
        "Y": df.get("Y"),
        "Z": df.get("Z"),
        "Stint": df["Stint"],
        "Compound": df["Compound"],
        "is_pit_lap": df["is_pit_lap"],
        "TrackTemp": df.get("TrackTemp"),
        "AirTemp": df.get("AirTemp"),
        "Rainfall": df.get("Rainfall"),
        "weather": df.get("weather"),
        "corner_id": df["corner_id"],
        "track_segment": df["track_segment"],
        "hard_brake": df["hard_brake"],
        "full_throttle": df["full_throttle"],
        "driver": df["driver"],
        "driver_name": df["driver_name"],
        "driver_code": df["driver_code"],
        "team": df["team"],
        "event": df["event"],
        "year": df["year"],
        "session": df["session"],
    })

    return out.sort_values("time_ms").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Export a FastF1 session to per-driver streaming CSVs")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--event", type=str, required=True, help='e.g. "Miami Grand Prix"')
    parser.add_argument("--session", type=str, default="R", help="R, Q, FP1, FP2, FP3, Sprint")
    parser.add_argument(
        "--output-dir", type=str,
        default="C:/telemetry-producer/src/main/java/f1producer/raw_telemetry_per_driver",
        help="Must match DATASET_ROOT in run_end_to_end.ps1 / docker-compose.yml",
    )
    parser.add_argument(
        "--cache-dir", type=str,
        default="C:/telemetry-producer/src/main/java/f1producer/f1_cache",
        help="Must match the cache_dir train_all_models.py looks for",
    )
    args = parser.parse_args()

    os.makedirs(args.cache_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)
    fastf1.Cache.enable_cache(args.cache_dir)

    print(f"Loading {args.year} {args.event} ({args.session})...")
    session = fastf1.get_session(args.year, args.event, args.session)
    session.load()

    event_slug = args.event.replace(" ", "_")
    weather = session.weather_data.copy() if session.weather_data is not None else None

    try:
        corner_distances, corner_numbers = build_corner_lookup(session)
    except Exception as e:
        print(f"[warn] circuit corner data unavailable ({e}) — track_segment will be 'Straight' throughout")
        corner_distances, corner_numbers = np.array([]), np.array([])

    written = 0
    for drv in session.drivers:
        print(f"Exporting driver {drv}...")
        out = export_driver(
            session, drv, args.event, event_slug, args.year, args.session,
            corner_distances, corner_numbers, weather,
        )
        if out is None:
            continue
        driver_code = out["driver_code"].iloc[0]
        filename = f"{args.year}_{event_slug}_{args.session}_{driver_code}.csv"
        path = os.path.join(args.output_dir, filename)
        out.to_csv(path, index=False)
        print(f"  -> {path} ({len(out)} rows)")
        written += 1

    print(f"\nDone. Wrote {written} driver CSVs to {args.output_dir}")
    if written == 0:
        print("No files written — check --year/--event/--session match a real, completed session.")


if __name__ == "__main__":
    main()