import duckdb
import os
import sys
import glob

# -----------------------------
# Configuration
# -----------------------------
# Configure these values depending on your requirements
TPCH_DB_FILE = "duckdb_data/tpch_1.duckdb"  # duckdb file will be created
TPCH_DATA_DIR = "../tpch/tpch_1"            # directory where .tbl files are located

# Allow overriding the data path via CLI
if len(sys.argv) > 1:
    TPCH_DATA_DIR = sys.argv[1]

# -----------------------------
# DuckDB Schema Definitions
# -----------------------------
SCHEMAS = {
    "region": """
        CREATE TABLE region (
            r_regionkey INTEGER,
            r_name VARCHAR,
            r_comment VARCHAR
        );
    """,
    "nation": """
        CREATE TABLE nation (
            n_nationkey INTEGER,
            n_name VARCHAR,
            n_regionkey INTEGER,
            n_comment VARCHAR
        );
    """,
    "supplier": """
        CREATE TABLE supplier (
            s_suppkey INTEGER,
            s_name VARCHAR,
            s_address VARCHAR,
            s_nationkey INTEGER,
            s_phone VARCHAR,
            s_acctbal DOUBLE,
            s_comment VARCHAR
        );
    """,
    "customer": """
        CREATE TABLE customer (
            c_custkey INTEGER,
            c_name VARCHAR,
            c_address VARCHAR,
            c_nationkey INTEGER,
            c_phone VARCHAR,
            c_acctbal DOUBLE,
            c_mktsegment VARCHAR,
            c_comment VARCHAR
        );
    """,
    "part": """
        CREATE TABLE part (
            p_partkey INTEGER,
            p_name VARCHAR,
            p_mfgr VARCHAR,
            p_brand VARCHAR,
            p_type VARCHAR,
            p_size INTEGER,
            p_container VARCHAR,
            p_retailprice DOUBLE,
            p_comment VARCHAR
        );
    """,
    "partsupp": """
        CREATE TABLE partsupp (
            ps_partkey INTEGER,
            ps_suppkey INTEGER,
            ps_availqty INTEGER,
            ps_supplycost DOUBLE,
            ps_comment VARCHAR
        );
    """,
    "orders": """
        CREATE TABLE orders (
            o_orderkey INTEGER,
            o_custkey INTEGER,
            o_orderstatus VARCHAR,
            o_totalprice DOUBLE,
            o_orderdate DATE,
            o_orderpriority VARCHAR,
            o_clerk VARCHAR,
            o_shippriority INTEGER,
            o_comment VARCHAR
        );
    """,
    "lineitem": """
        CREATE TABLE lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity DOUBLE,
            l_extendedprice DOUBLE,
            l_discount DOUBLE,
            l_tax DOUBLE,
            l_returnflag VARCHAR,
            l_linestatus VARCHAR,
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct VARCHAR,
            l_shipmode VARCHAR,
            l_comment VARCHAR
        );
    """,
}

# -----------------------------
# Main script
# -----------------------------
def load_tbl_file(con, table_name, file_path):
    print(f"Loading {table_name} from {file_path}")
    # DuckDB handles trailing pipes automatically
    con.execute(f"""
        COPY {table_name}
        FROM '{file_path}'
        (DELIMITER '|');
    """)


def main():
    if not os.path.isdir(TPCH_DATA_DIR):
        print(f"❌ Directory not found: {TPCH_DATA_DIR}")
        sys.exit(1)

    print(f"🦆 Creating DuckDB database: {TPCH_DB_FILE}")
    con = duckdb.connect(TPCH_DB_FILE)

    # Create tables
    for table, ddl in SCHEMAS.items():
        print(f"🧱 Creating table: {table}")
        con.execute(ddl)

    # Load data from .tbl files
    for table in SCHEMAS:
        file_path = os.path.join(TPCH_DATA_DIR, f"{table}.tbl")
        if os.path.exists(file_path):
            load_tbl_file(con, table, file_path)
        else:
            print(f"⚠️  File not found: {file_path} (skipping)")

    print("✅ TPCH setup complete. DuckDB database ready at:", TPCH_DB_FILE)


if __name__ == "__main__":
    main()
