def get_tpch_query_1(days_range):
    return f"""
    SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        lineitem
    WHERE
        l_shipdate <= DATE_SUB('1998-12-01', INTERVAL {days_range} DAY)
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
    """


def get_tpch_query_5():
    return """
    SELECT
        n_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue
    FROM
        customer
        JOIN orders ON c_custkey = o_custkey
        JOIN lineitem ON o_orderkey = l_orderkey
        JOIN supplier ON l_suppkey = s_suppkey
        JOIN nation n1 ON c_nationkey = n1.n_nationkey
        JOIN nation n2 ON s_nationkey = n2.n_nationkey
        JOIN region ON n1.n_regionkey = r_regionkey
    WHERE
        r_name = 'ASIA'
        AND o_orderdate >= DATE '1994-01-01'
        AND o_orderdate < DATE '1995-01-01'
        AND s_nationkey = c_nationkey
    GROUP BY
        n_name
    ORDER BY
        revenue DESC;
    """


query_dict = {
    "TPCH Query 1": get_tpch_query_1,
    "TPCH Query 5": get_tpch_query_5,
}