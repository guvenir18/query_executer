import re
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

from app.sampling_methods.adaptive_balanced_sampling import _coerce_numeric

_DB_KEYS = {
    "postgres": ("postgres", "postgre", "pgsql", "psql"),
    "duck": ("duck", "duckdb"),
    "mysql": ("mysql", "maria", "mariadb"),
}

def _server_to_engine(server: str) -> Optional[str]:
    if server is None:
        return None
    s = str(server).lower()
    for eng, keys in _DB_KEYS.items():
        if any(k in s for k in keys):
            return eng
    return None

def _find_filter_slot(rec: dict, filter_name: str) -> Optional[int]:
    """Return n where rec[f'filter_{n}'] == filter_name (case-insensitive, strip)."""
    if not filter_name:
        return None
    want = str(filter_name).strip().lower()
    for n in range(1, 10):  # support up to filter_9; adjust if you need more
        key = f"filter_{n}"
        if key in rec and rec[key] is not None:
            if str(rec[key]).strip().lower() == want:
                return n
    return None

def _encode_feature_series(raw_vals: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Encode raw feature values -> numeric range_value for the sampler.
    Priority:
      1) numeric (via _coerce_numeric)
      2) datetime (convert to POSIX seconds)
      3) categorical (factorize -> 1..K)
    Returns (range_value_numeric, raw_as_series)
    """
    # Try numeric
    num = _coerce_numeric(raw_vals)
    # If most values are numeric (or all), use them
    if num.notna().mean() >= 0.8:
        return num.astype(float), raw_vals

    # Try datetime
    dt = pd.to_datetime(raw_vals, errors="coerce", utc=True, infer_datetime_format=True)
    if dt.notna().mean() >= 0.8:
        secs = dt.view("int64") / 1e9  # convert ns -> seconds (float)
        return pd.to_numeric(secs, errors="coerce"), raw_vals

    # Fallback: categorical factorize (stable, 1-based)
    codes, uniques = pd.factorize(raw_vals.astype(str).fillna(""), sort=True)
    codes = pd.Series(codes + 1, index=raw_vals.index)  # 1..K
    return codes.astype(float), raw_vals


def load_runtime_from_json(
    records: List[dict],
    filter_name: str,
    extra_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Build the normalized DataFrame to sample **over `filter_name` values**:
      - range_value := numeric encoding of `val_n` where `filter_n == filter_name`
      - <filter_name> := raw `val_n` (kept for readability)
      - rows := corresponding `rows_n`
      - postgres_time / duck_time / mysql_time set from `server` + `runtime`
      - pass through any extra_cols; also keep _server/_database/_query
    """
    extra_cols = set(extra_cols or [])
    for rec in records:
        for k in rec.keys():
            if re.match(r"^(filter|val|rows)_\d+$", str(k), flags=re.I):
                extra_cols.add(k)

    rows_out = []
    raw_feature_vals = []

    for rec in records:
        n = _find_filter_slot(rec, filter_name)
        if n is None:
            continue

        engine = _server_to_engine(rec.get("server"))
        runtime_raw = rec.get("runtime", None)
        runtime_val = _coerce_numeric(pd.Series([runtime_raw])).iloc[0]

        val_raw  = rec.get(f"val_{n}", None)
        rows_raw = rec.get(f"rows_{n}", None)

        row = {
            "postgres_time": np.nan,
            "duck_time": np.nan,
            "mysql_time": np.nan,
            filter_name: val_raw,
            "rows": rows_raw,
            "_server": rec.get("server"),
            "_database": rec.get("database"),
            "_query": rec.get("query"),
        }
        if engine == "postgres":
            row["postgres_time"] = runtime_val
        elif engine == "duck":
            row["duck_time"] = runtime_val
        elif engine == "mysql":
            row["mysql_time"] = runtime_val

        for c in extra_cols:
            row[c] = rec.get(c, None)

        rows_out.append(row)
        raw_feature_vals.append(val_raw)

    if not rows_out:
        cols = [filter_name, "rows", "postgres_time", "duck_time", "mysql_time",
                "_server", "_database", "_query", "range_value"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows_out)

    feature_raw_series = pd.Series(raw_feature_vals, index=df.index)
    range_vals, _ = _encode_feature_series(feature_raw_series)
    df["range_value"] = range_vals

    df = df.sort_values("range_value", kind="mergesort").reset_index(drop=True)

    for c in ["postgres_time", "duck_time", "mysql_time"]:
        df[c] = _coerce_numeric(df[c])

    return df


def extract_filters(entry, max_filters=3):
    return [
        entry[f"filter_{i}"]
        for i in range(1, max_filters + 1)
        if entry.get(f"filter_{i}")
    ]
