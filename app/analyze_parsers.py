# TODO: IMPORTANT These are mostly AI generated so they might be parsing wrong
#            Make sure that these parsers are correct

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


def extract_runtime_and_filter_scans_postgres(explain_text: str, filters: List[str]) -> Dict[str, Any]:
    """
    explain_text: the raw text output of EXPLAIN ANALYZE (BUFFERS) from PostgreSQL
    filters: list of substrings to search for inside lines like: 'Filter: (<expr>)'
             e.g., ["o_orderdate", "r_name"]

    Returns:
        {
          "total_runtime": <float>,   # in seconds
          "filters": [
            {"variable": "<filter>", "total_rows": <int>}, ...
          ]
        }
    """

    # --- total runtime (seconds) ---
    total_runtime = 0.0
    m_rt = re.search(r"Execution Time:\s*([\d.]+)\s*ms", explain_text)
    if m_rt:
        total_runtime = float(m_rt.group(1)) / 1000.0

    # --- prepare accumulators ---
    scans: Dict[str, int] = {f: 0 for f in filters}
    norm_filters = [f.lower() for f in filters]

    # Regexes for parsing key lines
    re_node_header = re.compile(r"\(actual time=\s*[\d.]+\s*\.\.\s*[\d.]+\s*rows=(\d+)\s+loops=(\d+)\)")
    re_filter_line = re.compile(r"\bFilter:\s*(.+)")
    re_rows_removed = re.compile(r"\bRows Removed by Filter:\s*(\d+)")

    # State while streaming lines
    current_rows = 0
    current_loops = 1
    last_matched_filters: List[str] = []     # the *original* filter substrings that matched
    last_rows_output = 0                     # rows * loops for the node where Filter matched

    lines = explain_text.splitlines()

    def flush_pending_without_removed():
        """If we saw a Filter but no 'Rows Removed by Filter', count just rows_output."""
        nonlocal last_matched_filters, last_rows_output
        if last_matched_filters:
            for f in last_matched_filters:
                scans[f] += last_rows_output
            # clear pending
            last_matched_filters = []
            last_rows_output = 0

    for line in lines:
        # 1) Node header: update rows/loops; if we had a pending Filter (no rows-removed seen yet),
        #    flush it before starting a new node.
        m_head = re_node_header.search(line)
        if m_head:
            # new node encountered — flush any pending counts from previous Filter
            flush_pending_without_removed()

            current_rows = int(m_head.group(1))
            current_loops = int(m_head.group(2))
            continue

        # 2) Filter line: check for matches against provided substrings (case-insensitive)
        m_filt = re_filter_line.search(line)
        if m_filt:
            # starting a new filter within current node — flush any pending from previous filter first
            flush_pending_without_removed()

            filt_expr_l = m_filt.group(1).lower()

            matched: List[str] = []
            for f_in, f_raw in zip(norm_filters, filters):
                if f_in in filt_expr_l:
                    matched.append(f_raw)

            if matched:
                last_matched_filters = matched
                # rows_output is rows * loops from the most recent node header
                last_rows_output = current_rows * max(1, current_loops)
            continue

        # 3) Rows Removed by Filter (pair with the most recent Filter, if any)
        m_rr = re_rows_removed.search(line)
        if m_rr and last_matched_filters:
            removed = int(m_rr.group(1)) * max(1, current_loops)
            # rows scanned = rows_output + rows_removed
            for f in last_matched_filters:
                scans[f] += last_rows_output + removed
            # clear pending after consuming rows-removed
            last_matched_filters = []
            last_rows_output = 0
            continue

    # End of file: if there was a pending filter without a "Rows Removed..." line, flush it
    if last_matched_filters:
        for f in last_matched_filters:
            scans[f] += last_rows_output

    filters_list = [{"variable": k, "total_rows": v} for k, v in scans.items()]
    return {"total_runtime": total_runtime, "filters": filters_list}
