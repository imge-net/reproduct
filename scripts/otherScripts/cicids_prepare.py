import os, glob, argparse
import numpy as np
import pandas as pd

LEAK_COLS_CANDIDATES = [
    "Flow ID", "FlowID",
    "Source IP", "Src IP", "srcip", "src_ip",
    "Destination IP", "Dst IP", "dstip", "dst_ip",
    "Timestamp", "TimeStamp", "time", "timestamp",
]

def norm_cols(cols):
    # strip + keep original spacing (CICIDS has spaces)
    return [c.strip() for c in cols]

def guess_day_from_filename(path: str) -> str:
    base = os.path.basename(path).lower()
    if "monday" in base: return "Monday"
    if "tuesday" in base: return "Tuesday"
    if "wednesday" in base: return "Wednesday"
    if "thursday" in base: return "Thursday"
    if "friday" in base: return "Friday"
    return "Unknown"

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = norm_cols(df.columns)

    # drop leakage cols if present
    drop = [c for c in LEAK_COLS_CANDIDATES if c in df.columns]
    if drop:
        df = df.drop(columns=drop)

    # normalize label col name
    if "Label" not in df.columns:
        # some variants use 'label'
        if "label" in df.columns:
            df = df.rename(columns={"label":"Label"})
        else:
            raise ValueError("Could not find Label column in CICIDS chunk")

    # strip label values
    df["Label"] = df["Label"].astype(str).str.strip()

    # replace inf/-inf with nan
    df = df.replace([np.inf, -np.inf], np.nan)

    # Coerce non-label columns to numeric if they are object due to 'Infinity' strings etc.
    for c in df.columns:
        if c == "Label":
            continue
        if df[c].dtype == object:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_dir", required=True, help=".../data/cicids/raw/MachineLearningCSV")
    ap.add_argument("--out_dir", required=True, help=".../data/cicids/processed")
    ap.add_argument("--chunksize", type=int, default=200000)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    csvs = sorted(glob.glob(os.path.join(args.in_dir, "*.csv")))
    if not csvs:
        raise SystemExit(f"No CSV files found under: {args.in_dir}")

    print("[INFO] Found CSV files:", len(csvs))
    for p in csvs:
        print("  -", os.path.basename(p))

    # Write per-day parquet to keep memory stable
    for csv_path in csvs:
        day = guess_day_from_filename(csv_path)
        out_path = os.path.join(args.out_dir, f"cicids_{day}.parquet")

        print(f"\n[INFO] Processing {os.path.basename(csv_path)}  day={day}")
        first = True
        total_rows = 0
        writer = None

        # chunked read
        for chunk in pd.read_csv(csv_path, chunksize=args.chunksize, low_memory=False):
            chunk = clean_chunk(chunk)
            # drop rows with missing label
            chunk = chunk[chunk["Label"].notna()]

            # keep track
            total_rows += len(chunk)

            # append to parquet (via pyarrow)
            if first:
                chunk.to_parquet(out_path, index=False)
                first = False
            else:
                # append by reading existing parquet is expensive; instead, write temp parts then merge is better.
                # We'll write part files and merge later.
                part_path = out_path.replace(".parquet", f".part_{total_rows}.parquet")
                chunk.to_parquet(part_path, index=False)

        print(f"[OK] Wrote day parquet (and parts): {out_path}  rows~={total_rows}")

    print("\n[INFO] Done. Next: merge parts per day (script will handle in baseline).")

if __name__ == "__main__":
    main()
