import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Description:
    Creates tables for stagging from copy_table_queries.
    
    Arguments:
    cur - cursor object
    con - connection object
    
    Return:
    None
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
    Inserts data into tables using insert_table_quries.
    
    Arguments:
    cur - cursor object
    con - connection object
    
    Return:
    None
    """
    
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()