from psycopg_pool import AsyncConnectionPool
from app.config import load_config

config = load_config()


def create_postgres_pool() -> AsyncConnectionPool:
    """
    Creates and returns an async Postgres connection pool.
    """
    conninfo = (
        f"host={config.database.postgres.host} "
        f"port={config.database.postgres.port} "
        f"dbname={config.database.postgres.database} "
        f"user={config.database.postgres.username} "
        f"password={config.database.postgres.password}"
    )
    return AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=10)