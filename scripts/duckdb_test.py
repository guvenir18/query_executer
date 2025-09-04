import os

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

result = con.execute(query_5).fetchall()
print("Wrote profile to", os.path.abspath("duck_profile.json"))
print(result)
