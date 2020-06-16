import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
Drop database tables from drop_table_queries
"""

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

""" 
create database tables from create_table_queries 
"""

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("connected")
    cur = conn.cursor()

    print("Drop Table if already exist")
    drop_tables(cur, conn)
    
    print("Create new table")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()