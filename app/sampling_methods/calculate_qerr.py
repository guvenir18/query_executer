import numpy as np
import pandas as pd

# ---------- helpers: feature/runtime extraction ----------

def _encode_feature_for_model(raw_vals: pd.Series) -> pd.Series:
    """
    Convert the raw feature values (e.g., 'o_orderdate') into numeric X for modeling.
    Priority:
      1) numeric coercion
      2) datetime -> POSIX seconds
      3) categorical -> stable codes 1..K
    """
    # try numeric
    num = pd.to_numeric(raw_vals, errors="coerce")
    if num.notna().mean() >= 0.8:
        return num.astype(float)

    # try datetime
    dt = pd.to_datetime(raw_vals, errors="coerce", utc=True, infer_datetime_format=True)
    if dt.notna().mean() >= 0.8:
        # convert to seconds since epoch as float
        return (dt.view("int64") / 1e9).astype(float)

    # fallback categorical factorization (stable if we sort uniques)
    codes, uniques = pd.factorize(raw_vals.astype(str).fillna(""), sort=True)
    return pd.Series(codes + 1, index=raw_vals.index, dtype=float)


def _pick_runtime_column(df: pd.DataFrame, engine: str | None = "auto") -> str:
    """
    Decide which runtime column to use.
    - engine in {"postgres","duck","mysql"} -> fixed column
    - "auto" -> choose the column with the most non-null values
    """
    engine = (engine or "auto").lower()
    name_map = {
        "postgres": "postgres_time",
        "duck": "duck_time",
        "mysql": "mysql_time",
    }
    if engine in name_map:
        return name_map[engine]

    # auto: pick the densest column
    cands = ["postgres_time", "duck_time", "mysql_time"]
    present = [c for c in cands if c in df.columns]
    if not present:
        raise ValueError("No runtime columns found (expected postgres_time/duck_time/mysql_time).")
    densest = max(present, key=lambda c: df[c].notna().sum())
    return densest


def _get_xy(df: pd.DataFrame, filter_name: str, engine: str | None = "auto") -> tuple[pd.Series, pd.Series]:
    """
    Build (X, Y) from a dataframe:
      X: numeric-encoded values of the given filter column (e.g., 'o_orderdate')
      Y: chosen runtime column
    Rows with Y NaN are dropped to fit/predict robustly.
    """
    if filter_name not in df.columns:
        raise KeyError(f"'{filter_name}' column not found in dataframe.")
    x_raw = df[filter_name]
    x_num = _encode_feature_for_model(x_raw)

    y_col = _pick_runtime_column(df, engine)
    y = pd.to_numeric(df[y_col], errors="coerce")

    # Keep alignment; drop rows where y is NaN (cannot fit/predict)
    mask = y.notna()
    return x_num[mask], y[mask]

# ---------- polynomial fit & evaluation ----------

def fit_polynomial_on_sample(
    sampled_df: pd.DataFrame,
    filter_name: str,
    engine: str | None = "auto",
    degree: int = 2
) -> np.poly1d:
    """
    Fit a polynomial (least squares) Y ~ poly(X) on the sampled rows.
    X = numeric encoding of 'filter_name' column.
    Y = runtime (chosen via engine).
    Returns a numpy.poly1d model.
    """
    X, Y = _get_xy(sampled_df, filter_name=filter_name, engine=engine)
    if len(X) < degree + 1:
        # fall back to a lower degree if too few points
        degree = max(0, min(degree, len(X) - 1))
    if len(X) == 0:
        raise ValueError("No valid (X,Y) pairs in sampled_df to fit the model.")
    coeffs = np.polyfit(X.values, Y.values, deg=degree)
    return np.poly1d(coeffs)


def compute_qerr(actual: float, predicted: float) -> float:
    """Q-error as specified."""
    try:
        if actual > 0 and predicted > 0:
            a = float(actual)
            p = float(predicted)
            return max(a / p, p / a)
        return np.nan
    except Exception:
        return np.nan


def predict_and_qerr_for_all(
    full_df: pd.DataFrame,
    model: np.poly1d,
    filter_name: str,
    engine: str | None = "auto"
) -> pd.DataFrame:
    """
    For every row in full_df:
      - build X from 'filter_name'
      - predict runtime with the fitted poly
      - compute Q-error against the chosen actual runtime column
    Returns a dataframe with columns:
      [filter_name, 'x_numeric', 'actual', 'predicted', 'qerr', 'runtime_column', ... passthrough ids]
    """
    if filter_name not in full_df.columns:
        raise KeyError(f"'{filter_name}' column not found in full_df.")

    # X numeric for ALL rows (don’t drop NaNs here — we’ll handle prediction NaNs)
    x_numeric = _encode_feature_for_model(full_df[filter_name])

    # pick actual runtime column
    y_col = _pick_runtime_column(full_df, engine)
    actual = pd.to_numeric(full_df[y_col], errors="coerce")

    # predict
    predicted = pd.Series(np.nan, index=full_df.index, dtype=float)
    # predict only where we have a finite X
    valid_x = x_numeric.notna() & np.isfinite(x_numeric)
    predicted.loc[valid_x] = model(x_numeric.loc[valid_x].values)

    # qerr
    qerr = [
        compute_qerr(a, p) if np.isfinite(a) and np.isfinite(p) else np.nan
        for a, p in zip(actual.values, predicted.values)
    ]
    qerr = pd.Series(qerr, index=full_df.index, dtype=float)

    out_cols = [filter_name, "x_numeric", "actual", "predicted", "qerr"]
    out = pd.DataFrame({
        filter_name: full_df[filter_name],
        "x_numeric": x_numeric,
        "actual": actual,
        "predicted": predicted,
        "qerr": qerr,
    })

    # Optional helpful columns for debugging/tracing
    for c in ["_server", "_database", "_query"]:
        if c in full_df.columns:
            out[c] = full_df[c]
    out["runtime_column"] = y_col
    return out


def summarize_qerr(qerr_series: pd.Series) -> dict:
    """Return common Q-error summary stats."""
    q = qerr_series.dropna()
    if q.empty:
        return {
            "count": 0,
            "median_qerr": np.nan,
            "p90_qerr": np.nan,
            "p95_qerr": np.nan,
            "max_qerr": np.nan,
        }
    return {
        "count": int(q.shape[0]),
        "median_qerr": float(np.median(q)),
        "p90_qerr": float(np.percentile(q, 90)),
        "p95_qerr": float(np.percentile(q, 95)),
        "max_qerr": float(np.max(q)),
    }
