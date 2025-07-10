import aiomysql
from app.config import load_config

config = load_config()

async def create_mysql_pool():
    """
    Creates a connection pool for MySQL
    """
    return await aiomysql.create_pool(
        host=config.database.mysql.host,
        user=config.database.mysql.user,
        password=config.database.mysql.password,
        db=config.database.mysql.db,
        minsize=1,
        maxsize=10,
        autocommit=True,
    )
