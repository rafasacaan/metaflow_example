from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
import traceback
import sqlalchemy

class SnowflakeOperator():
    def __init__(self, user, password, schema, account, warehouse, database, role):
        self.engine = create_engine(URL(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema
        ))

    def df_from_query(self, query: str = None) -> pd.DataFrame:
        #TODO: optimize this using mp
        conn = self.engine.connect()
        results = pd.DataFrame()
        for df_chunk in pd.read_sql_query(query, con=conn, chunksize=100000):
            results = df_chunk if results.empty else results.append(df_chunk)
        conn.close()
        return results

    def df_to_snowflake(self, dataframe: pd.DataFrame,
                              destination_table: str = None,
                              if_exists: str = "replace") -> None:
        if if_exists not in ['append','replace']:
            raise ValueError("if_exists must be 'replace' or 'append'")
        conn = self.engine.connect()
        dataframe.to_sql(name=destination_table,
                         con=conn,
                         if_exists=if_exists,
                         index=False)
        conn.close()


    def run_query(self, query: str):
        """
        Executes a query in Snowflake 
        (for example: create/modify a table without bringing it into the local machine)
        """
        conn = self.engine.connect()
        try:
            result = conn.execute(query)
        finally:
            conn.close()
        return result
