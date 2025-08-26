# adaptive_balanced_sampling.py (exact target_n, equal batches, NaN-safe, aligned ranking, random tiebreak)
from __future__ import annotations
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import numpy as np
import pandas as pd
import re

from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel
from sklearn.preprocessing import StandardScaler

# ---------- Utilities ----------


def _coerce_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(r"\s+", "", regex=True)
    def fix_one(x: str) -> str:
        if x == "" or x.lower() in {"nan", "none"}:
            return ""
        if "," in x and "." in x:
            x = x.replace(",", "")
        elif "," in x and "." not in x:
            x = x.replace(",", ".")
        x = re.sub(r"[^0-9\.\-eE+]", "", x)
        return x
    s = s.apply(fix_one)
    return pd.to_numeric(s, errors="coerce")


def robust_stats(x: np.ndarray) -> tuple[float, float]:
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        return 0.0, 1.0
    med = np.nanmedian(x)
    mad_raw = np.nanmedian(np.abs(x - med))
    mad = 1.4826 * mad_raw if mad_raw > 0 else (np.nanstd(x) if np.nanstd(x) > 0 else 1.0)
    if not np.isfinite(med): med = 0.0
    if not np.isfinite(mad) or mad == 0: mad = 1.0
    return med, mad


def stratified_time_buckets(N: int, K: int) -> np.ndarray:
    bins = np.linspace(0, N, K + 1, dtype=int)
    return np.digitize(np.arange(N), bins[1:], right=False)  # 0..K-1


# ---- tie-break yardımcı: eşit skorları seed'le rastgele kır ----
def _rank_desc_with_tiebreak(scores: np.ndarray, rng: np.random.RandomState) -> np.ndarray:
    s = np.nan_to_num(scores, nan=-1e-18)  # NaN'ları -∞ kabul et
    # skala bağımsız minik jitter (deterministik)
    jitter = rng.standard_normal(len(s)) * (1e-12 if np.isfinite(np.std(s)) else 1e-12)
    key = s + jitter
    return np.argsort(-key)

# ---------- Adaptive Balanced Sampling ----------


def sample_adaptive_balanced(df: pd.DataFrame,
                             target_ratio: float = 0.10,
                             target_n: Optional[int] = None,
                             seed: int = 42,
                             strata_K: int = 12,
                             batches: Optional[int] = None,
                             lambda_weight: float = 0.7,
                             kappa: float = 1.8) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    N = len(df)
    n_target = int(round(N * target_ratio)) if target_n is None else int(target_n)

    if batches is None:
        if n_target <= 40:
            batches = 2
        elif n_target <= 100:
            batches = 3
        elif n_target <= 180:
            batches = 5
        else:
            batches = 6

    base = n_target // batches
    batch_sizes = np.full(batches, base, dtype=int)
    batch_sizes[: (n_target % batches)] += 1  # örn. 168,6 -> 28'er

    strata = stratified_time_buckets(N, K=strata_K)

    all_idx = np.arange(N)
    unseen = set(all_idx.tolist())
    seen: List[int] = []
    records: List[Tuple[int, int, float]] = []  # (_idx, batch, acq_total)

    # -------- Seed batch: stratified random, tam bsize --------
    seed_size = int(batch_sizes[0])
    lens = np.array([np.sum(strata == s) for s in range(strata_K)], dtype=int)
    raw = lens * (float(seed_size) / max(lens.sum(), 1))
    baseq = np.floor(raw).astype(int)
    rem = raw - baseq
    quota = baseq.copy()
    missing = max(0, seed_size - quota.sum())
    if missing > 0:
        quota[np.argsort(-rem)[:missing]] += 1
    seed_idx: List[int] = []
    for s in range(strata_K):
        in_s = np.where(strata == s)[0]
        need = int(min(quota[s], len(in_s)))
        if need > 0:
            pick = rng.choice(in_s, size=need, replace=False)
            seed_idx.extend(pick.tolist())
    if len(seed_idx) < seed_size:
        rem = np.setdiff1d(all_idx, np.array(seed_idx, dtype=int))
        add = rng.choice(rem, size=(seed_size - len(seed_idx)), replace=False)
        seed_idx.extend(add.tolist())
    seed_idx = np.array(seed_idx, dtype=int)
    for ii in seed_idx.tolist():
        records.append((ii, 1, np.nan))
        seen.append(ii)
        unseen.discard(ii)

    # -------- Özellikler --------
    X_day = df["range_value"].values.reshape(-1, 1).astype(float)
    scaler = StandardScaler().fit(X_day)
    X_scaled = scaler.transform(X_day)

    # log hedefler & robust parametreler
    def _log1p(v):
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.log1p(v.astype(float))
    log_cols: Dict[str, np.ndarray] = {}
    robust_params: Dict[str, Tuple[float, float]] = {}
    for cname in ["postgres_time", "duck_time", "mysql_time"]:
        y = df[cname].values
        logy = _log1p(y)
        log_cols[cname] = logy
        med, mad = robust_stats(logy[~np.isnan(logy)])
        robust_params[cname] = (med, mad)

    kernel = 1.0 * RBF(length_scale=0.5) + WhiteKernel(noise_level=0.1)

    # -------- Kalan batch'ler --------
    for b_id in range(2, batches + 1):
        bsize = int(batch_sizes[b_id - 1])
        if bsize <= 0 or len(unseen) == 0:
            continue

        mu_map: Dict[str, np.ndarray] = {}
        sigma_map: Dict[str, np.ndarray] = {}
        for cname in ["postgres_time", "duck_time", "mysql_time"]:
            ylog = log_cols[cname]
            train_idx = [i for i in seen if not np.isnan(ylog[i])]
            if len(train_idx) < 3:
                mu_map[cname] = np.full(N, np.nan)
                sigma_map[cname] = np.full(N, np.nan)
                continue
            X_tr = X_scaled[train_idx]
            y_tr = ylog[train_idx]
            gpr = GaussianProcessRegressor(kernel=kernel, optimizer=None, normalize_y=True, alpha=1e-4)
            gpr.fit(X_tr, y_tr)
            mu, std = gpr.predict(X_scaled, return_std=True)
            mu_map[cname] = mu
            sigma_map[cname] = std

        mu_tilde_list, sigma_tilde_list = [], []
        for cname in ["postgres_time", "duck_time", "mysql_time"]:
            mu = mu_map.get(cname)
            sd = sigma_map.get(cname)
            med, mad = robust_params[cname]
            if mu is None or np.all(np.isnan(mu)):
                mu_tilde_list.append(np.full(N, np.nan))
                sigma_tilde_list.append(np.full(N, np.nan))
            else:
                mu_tilde_list.append((mu - med) / (mad if mad > 0 else 1.0))
                sigma_tilde_list.append(sd / (mad if mad > 0 else 1.0))
        mu_tilde = np.vstack(mu_tilde_list)
        sigma_tilde = np.vstack(sigma_tilde_list)

        ucb_mat = mu_tilde + kappa * sigma_tilde

        if np.all(~np.isfinite(sigma_tilde)):
            a_unc_all = np.zeros(N)
        else:
            a_unc_all = np.sqrt(np.nanmean(np.square(sigma_tilde), axis=0))

        if np.all(~np.isfinite(ucb_mat)):
            a_ucb_all = np.full(N, -1e18)
        else:
            a_ucb_all = np.nanmax(ucb_mat, axis=0)

        a_all = lambda_weight * a_unc_all + (1 - lambda_weight) * a_ucb_all

        # -- ÖNEMLİ: unseen_list'i sıralama (index bias olmasın) --
        unseen_list = np.array(list(unseen), dtype=int)
        if len(unseen_list) == 0:
            break

        strata_unseen = strata[unseen_list]
        len_unseen_per_stratum = np.array([np.sum(strata_unseen == s) for s in range(strata_K)], dtype=int)
        raw = len_unseen_per_stratum * (float(bsize) / max(len_unseen_per_stratum.sum(), 1))
        baseq = np.floor(raw).astype(int)
        rem = raw - baseq
        quota = baseq.copy()
        missing = max(0, bsize - quota.sum())
        if missing > 0:
            quota[np.argsort(-rem)[:missing]] += 1
        quota = np.minimum(quota, len_unseen_per_stratum)

        chosen: List[int] = []
        for s in range(strata_K):
            need = int(quota[s])
            if need <= 0:
                continue
            mask = (strata_unseen == s)
            cand_idx = unseen_list[mask]
            if len(cand_idx) == 0:
                continue
            cand_scores = a_all[cand_idx]
            # tie-break'li sıralama
            order = _rank_desc_with_tiebreak(cand_scores, rng)
            pick = cand_idx[order[:min(need, len(cand_idx))]]
            chosen.extend(pick.tolist())

        # eksik kaldıysa global en yüksek skorlarla tamamla (tie-break'li)
        if len(chosen) < bsize:
            remaining = np.setdiff1d(unseen_list, np.array(chosen, dtype=int))
            if len(remaining) > 0:
                rem_scores = a_all[remaining]
                order = _rank_desc_with_tiebreak(rem_scores, rng)
                add = remaining[order[:(bsize - len(chosen))]]
                chosen.extend(add.tolist())

        # fazla olduysa en düşük skorluları at (tie-break'li sıralama)
        if len(chosen) > bsize:
            chosen_arr = np.array(chosen, dtype=int)
            chosen_scores = a_all[chosen_arr]
            order = _rank_desc_with_tiebreak(chosen_scores, rng)
            chosen = chosen_arr[order[:bsize]].tolist()

        for ii in chosen:
            score = float(a_all[ii]) if np.isfinite(a_all[ii]) else np.nan
            records.append((ii, b_id, score))
            seen.append(ii)
            unseen.discard(ii)

    # toplam tam target_n: seen'i tamla/kırp
    if len(seen) > n_target:
        seen = rng.choice(np.array(seen, dtype=int), size=n_target, replace=False).tolist()
    elif len(seen) < n_target and len(unseen) > 0:
        add = rng.choice(np.array(list(unseen), dtype=int), size=min(n_target - len(seen), len(unseen)), replace=False).tolist()
        seen.extend(add)

    sel_idx = np.array(seen[:n_target], dtype=int)

    # --- Çıktı: sel_idx baz alınır; meta varsa eşleştirilir (index hizası düzeltilmiş) ---
    meta = pd.DataFrame(records, columns=["_idx", "batch", "acq_total"]).drop_duplicates("_idx", keep="first")
    meta_aligned = meta.set_index("_idx").reindex(sel_idx)

    batch_vals = meta_aligned["batch"].to_numpy()
    scores = meta_aligned["acq_total"].to_numpy()

    order = pd.Series(np.nan_to_num(scores, nan=-1e-18)).groupby(batch_vals).rank(
        ascending=False, method="dense"
    ).to_numpy()

    out = df.iloc[sel_idx].copy()
    out.insert(0, "batch", batch_vals)
    out.insert(1, "order_in_batch", order)
    out.insert(2, "acq_total", scores)
    out.insert(0, "selection_order", np.arange(1, len(out) + 1))
    return out
