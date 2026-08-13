#!/usr/bin/env python3
import warnings, os, sys
warnings.filterwarnings("ignore")
sys.stderr = open(os.devnull, "w")
import pickle
import numpy as np
import pandas as pd


MODEL_DIR = os.getenv("MODEL_DIR", "/var/lib/clickhouse/user_scripts/models")
MODEL_PATH = os.path.join(MODEL_DIR, "laptime_model.pkl")
FEATS_PATH = os.path.join(MODEL_DIR, "laptime_feats.pkl")

with open(MODEL_PATH, "rb") as f:
    MODEL = pickle.load(f)
with open(FEATS_PATH, "rb") as f:
    FEATS = pickle.load(f)

INPUT_COLUMNS = [
    "avg_speed",
    "compound_enc",
    "tire_age",
    "tire_age_pct",
    "track_type_enc",
    "rel_speed_delta",
    "pace_drop_3",
    "avg_throttle",
    "avg_brake",
    "hard_brake_rate",
    "avg_drs",
    "avg_rpm",
    "track_temp",
    "air_temp",
    "rainfall",
    "laps_remaining",
    "LapNumber",
    "race_pct",
    "speed_rank_pct",
    "delta_vs_field",
    "gap_to_below_proxy",
    "teammate_pitted",
]


def parse_line(line: str):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != len(INPUT_COLUMNS):
        return None
    try:
        vals = [float(x) for x in parts]
        return dict(zip(INPUT_COLUMNS, vals))
    except ValueError:
        return None


def main():
    rows = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        rows.append(parse_line(line))

    if not rows:
        return

    valid = [(i, r) for i, r in enumerate(rows) if r is not None]
    out = ["0.0"] * len(rows)

    if valid:
        idxs, dicts = zip(*valid)
        df = pd.DataFrame(dicts)[FEATS].fillna(0)
        preds = MODEL.predict(df)
        for idx, v in zip(idxs, preds):
            if not np.isfinite(v):
                v = 0.0
            out[idx] = f"{max(float(v), 0.0):.9f}"

    sys.stdout.write("\n".join(out))


if __name__ == "__main__":
    main()
