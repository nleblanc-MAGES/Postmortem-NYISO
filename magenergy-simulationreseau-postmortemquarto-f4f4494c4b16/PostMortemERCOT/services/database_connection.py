from Snowflake_Natif_Connector import conn_python_snowflake as ntf
from typing import Any


def init_connection(warehouse: str='LARGE_COMPUTE_WAREHOUSE') -> Any:
    """
    Establishes a connection to the Snowflake database.

    Returns:
        Connection object: A connection to the specified Snowflake database.
    """
    return ntf.establishconnection(warehouse, "MAGSNOWFLAKE", "DAYZER")