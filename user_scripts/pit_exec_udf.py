#!/usr/bin/env python3
import warnings, os, sys
warnings.filterwarnings("ignore")
sys.stderr = open(os.devnull, "w")
import pickle
import numpy as np
import pandas as pd

MODEL_DIR = os.getenv("MODEL_DIR", "/var/lib/clickhouse/user_scripts/models")
with open(os.path.join(MODEL_DIR, "pit_model.pkl"), "rb") as f: MODEL = pickle.load(f)
with open(os.path.join(MODEL_DIR, "pit_feats.pkl"), "rb") as f: FEATS = pickle.load(f)

INPUT_COLUMNS = FEATS

def parse_line(line):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != len(INPUT_COLUMNS): return None
    try: return dict(zip(INPUT_COLUMNS, [float(x) for x in parts]))
    except ValueError: return None

def main():
    rows = [parse_line(l.strip()) for l in sys.stdin if l.strip()]
    if not rows: return
    valid = [(i,r) for i,r in enumerate(rows) if r is not None]
    out = ["0.0"] * len(rows)
    if valid:
        idxs, dicts = zip(*valid)
        df = pd.DataFrame(dicts)[FEATS].fillna(0)
        probs = MODEL.predict_proba(df)[:,1]
        for idx,p in zip(idxs,probs):
            out[idx] = f"{min(max(float(p),0.0),1.0):.9f}"
    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()

if __name__ == "__main__": main()
