import duckdb
import pandas as pd

# Connect to DuckDB
con = duckdb.connect("db_store.duckdb")  # persistent database

def register_data(df: pd.DataFrame):
    """Register or replace the 'data' table."""
    
    # First, try to drop the table (the final object from a successful run)
    con.execute("DROP TABLE IF EXISTS data")

    # Then, drop the view (in case the last run was interrupted)
    con.execute("DROP VIEW IF EXISTS data") 
    
    con.register("data", df)
    con.execute("CREATE TABLE data AS SELECT * FROM data")

def run_query(query: str):
    """Run a custom SQL query."""
    try:
        result = con.execute(query).fetchdf()
        return result
    except Exception as e:
        raise e
