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
#q = 'select * from animal_medical_info;'
#print(run_query(q))


# inside EC2
# import psycopg2
# import pandas as pd
#
#
# dbname = 'postgres'
# user = 'postgres'
# host = 'database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com'
# password = 'fureverdb'
# try:
#     conn = psycopg2.connect("dbname='postgres' user='postgres' host='database-1.cu3ixi7c6kol.us-west-2.rds.amazonaws.com' password='fureverdb'")
#     print("Status: Connected to database")
# except:
#     print("I am unable to connect to the database")
#
# cur = conn.cursor()
# cur.execute("""SELECT * from animal_info limit 10; """)
# rows = cur.fetchall()
# print("\nShow me the rows:\n")
# for row in rows:
#     print("   ", row[0])(python3)