import pyodbc
import pandas as pd
from typing import Sequence, Any


class DWClient:
    """
    Client de infraestrutura para acesso ao Data Warehouse (SQL Server).
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def fetch_df(
        self,
        query: str,
        params: Sequence[Any] | None = None
    ) -> pd.DataFrame:
        with pyodbc.connect(self.connection_string) as conn:
            return pd.read_sql(query, conn, params=params)