import json
import re
from typing import Dict, Any, List


def parse_analyze_mysql(plan_text: str, filter_vars: list):
    # Updated regex to also match filters with (never executed)
    pattern = re.compile(
        r"Filter: \((.*?)\)\s*\((?:(?:actual time=(.*?) rows=([^\s]+) loops=([^\)]+))|never executed)\)",
        re.DOTALL
    )

    results = []
    for match in pattern.findall(plan_text):
        condition = match[0]
        time_range = match[1]
        rows = match[2]
        loops = match[3]

        condition_lower = condition.lower()
        for var in filter_vars:
            if var.lower() in condition_lower:
                if time_range:  # Means it was executed
                    total_rows = int(float(rows) * float(loops))
                else:
                    total_rows = 0

                results.append({
                    "variable": var,
                    "total_rows": total_rows,
                })
                break

    return results


def extract_total_runtime(plan_text: str) -> float:
    """
    Extracts the total runtime (in milliseconds) from the top-level node
    in MySQL EXPLAIN ANALYZE output by taking the right-hand value
    """
    match = re.search(r"actual time=\d+\.?\d*\.\.(\d+\.?\d*)", plan_text)
    if match:
        return float(match.group(1))
    return 0.0


def extract_runtime_and_filter_scans_duckdb(profile_json: str, filters: List[str]) -> Dict[str, Any]:
    """
    profile_json: JSON string of the DuckDB profile (what EXPLAIN ANALYZE JSON emits)
    filters: list of substrings to look for inside extra_info["Filters"], e.g. ["o_orderdate"]

    Returns:
        {
          "total_runtime": <float>,   # from profile["latency"] if present, else 0.0
          "filters": [
            {"variable": "<filter>", "total_rows": <int>}, ...
          ]
        }
    """
    # Parse JSON string → Python dict (jsonb-like structure)
    try:
        profile: Dict[str, Any] = json.loads(profile_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid profile JSON: {e}") from e

    # total runtime
    total_runtime = float(profile.get("latency", 0.0))

    # prepare accumulator
    scans: Dict[str, int] = {f: 0 for f in filters}
    norm_filters = [f.lower() for f in filters]

    def walk(node: Dict[str, Any]):
        # check Filters in extra_info
        extra = node.get("extra_info") or {}
        filt = extra.get("Filters")
        if isinstance(filt, str):
            filt_l = filt.lower()
            for f_in, f_raw in zip(norm_filters, filters):
                if f_in in filt_l:
                    scans[f_raw] += int(node.get("operator_rows_scanned", 0))

        # recurse
        for child in (node.get("children") or []):
            walk(child)

    walk(profile)

    # dict → list of objects
    filters_list = [{"variable": k, "total_rows": v} for k, v in scans.items()]

    return {"total_runtime": total_runtime, "filters": filters_list}

