import json
import os
from typing import Dict, Any, List

import duckdb

# Connect to DuckDB (in-memory DB or file-based if you want persistence)
con = duckdb.connect(database='..\\duckdb_data\\tpch_1.duckdb', read_only=False)

con.execute("PRAGMA enable_profiling;")
con.execute("SET enable_profiling = 'json';")
con.execute("SET profiling_output = 'out.json';")
con.execute("SET profile_output = 'out.json';")

# If you already loaded TPCH tables into DuckDB, you can run directly
query_5 = """
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC;
    

"""

def extract_runtime_and_filter_scans(profile: Dict[str, Any], filters: List[str]) -> Dict[str, Any]:
    """
    profile: parsed DuckDB JSON profile (the dict you posted)
    filters: list of substrings to look for inside extra_info["Filters"], e.g. ["o_orderdate"]
    Returns:
        {
          "total_runtime": <float>,         # from profile["latency"] if present, else 0.0
          "filters": { "<filter>": <int>, ... }  # sum of operator_rows_scanned on nodes whose Filters contain the substring
        }
    """
    # total runtime
    total_runtime = float(profile.get("latency", 0.0))

    # prepare accumulator
    scans: Dict[str, int] = {f: 0 for f in filters}
    # normalize filters for case-insensitive matching
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
        for child in node.get("children", []) or []:
            walk(child)

    walk(profile)
    return {"total_runtime": total_runtime, "filters": scans}

result = con.execute(query_5).fetchall()
print("Wrote profile to", os.path.abspath("duck_profile.json"))
print(result)

with open("out.json", "r") as f:
    profile = json.load(f)

filters = ["o_orderdate"]   # you can add more filter names here
result = extract_runtime_and_filter_scans(profile, filters)
print(result)

