import psycopg2

import pandas as pd
import pandas.io.sql as sqlio

try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com' password='fureverdb'")
    query = conn.cursor()
    print("Status: Connected to database")
except:
    print("Status: Failed to connect to database")


def run_query(query):
    print("Status: Running query")
    results = sqlio.read_sql_query(query, conn)
    print("Status: COMPLETED")
    return results
