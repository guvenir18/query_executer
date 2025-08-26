# stratified_time_sampling.py  (Hamilton kota + 0-based strata fix)
from __future__ import annotations
from pathlib import Path
from typing import Optional
import numpy as np
import pandas as pd


# ---------- Stratified time-bucket sampling ----------

def stratified_time_buckets(N: int, K: int) -> np.ndarray:
    bins = np.linspace(0, N, K + 1, dtype=int)
    # 0..K-1 etiket
    return np.digitize(np.arange(N), bins[1:], right=False)


def _hamilton_quotas(strata: np.ndarray, K: int, N: int, n_target: int) -> np.ndarray:
    """Largest Remainder (Hamilton) kota dağıtımı (0-bazlı strata)."""
    len_s = np.array([np.sum(strata == s) for s in range(K)], dtype=int)
    raw = len_s * (float(n_target) / N)
    base = np.floor(raw).astype(int)
    rem  = raw - base
    quota = base.copy()
    missing = int(n_target - quota.sum())
    if missing > 0:
        order = np.argsort(-rem)          # en büyük kalanlara +1
        quota[order[:missing]] += 1
    quota = np.minimum(quota, len_s)
    excess = int(quota.sum() - n_target)
    if excess > 0:
        order = np.argsort(rem)           # küçük kalanlardan kes
        for s_idx in order[:excess]:
            if quota[s_idx] > 0:
                quota[s_idx] -= 1
    return quota


def sample_stratified(df: pd.DataFrame,
                      target_ratio: float = 0.10,
                      target_n: Optional[int] = None,
                      K: int = 12,
                      seed: int = 42,
                      add_bucket: bool = False) -> pd.DataFrame:
    """
    Zamanı K parçaya böl, Hamilton ile kota dağıt, her parçadan rastgele seç.
    target_n verilirse tam olarak o kadar satır döndürür; yoksa ratio*N yuvarlanır.
    """
    rng = np.random.RandomState(seed)
    N = len(df)
    n_target = int(round(N * target_ratio)) if target_n is None else int(target_n)

    strata = stratified_time_buckets(N, K=K)   # 0..K-1
    quota = _hamilton_quotas(strata, K, N, n_target)

    # Rastgele seçim (global sort yok)
    picks = []
    for s in range(K):
        in_s = np.where(strata == s)[0]
        k_s = int(quota[s])
        if k_s <= 0 or len(in_s) == 0:
            continue
        sel = rng.choice(in_s, size=k_s, replace=False)
        picks.extend(sel.tolist())

    picks = np.array(picks, dtype=int)

    # Sayıyı tam n_target yap: rastgele kırp/doldur
    if len(picks) > n_target:
        picks = rng.choice(picks, size=n_target, replace=False)
    elif len(picks) < n_target:
        remaining = np.setdiff1d(np.arange(N), picks)
        add = rng.choice(remaining, size=(n_target - len(picks)), replace=False)
        picks = np.concatenate([picks, add])

    out = df.iloc[picks].copy()

    # Kolon sıralaması: selection_order, (bucket?), day, extra..., times...
    time_cols = [c for c in ["postgres_time", "duck_time", "mysql_time"] if c in out.columns]
    extra_cols = [c for c in out.columns if c not in (["range_value"] + time_cols)]
    ordered = ["range_value"] + extra_cols + time_cols
    out = out[ordered]

    if add_bucket:
        out.insert(1, "bucket", strata[picks] + 1)  # 1-bazlı gösterim

    out.insert(0, "selection_order", np.arange(1, len(out) + 1))
    return out
