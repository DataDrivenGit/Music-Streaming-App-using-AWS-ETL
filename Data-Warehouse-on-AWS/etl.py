import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,test_queries
import pandas as pd


"""
Load/Copy data from storage
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
Insert data into Fact/Dimention tables
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
Test data warehouse tables for data records
"""
def test_tables(conn):
    for query in test_queries:
        data = pd.read_sql(query,conn)
        print(data)

        
"""
main function to establish connection and call ETL functions to execute
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading Tables")
    load_staging_tables(cur, conn)
    
    print("Inserting data")
    insert_tables(cur, conn)
    
    print("Test data")
    test_tables(conn)

    conn.close()


if __name__ == "__main__":
    main()