import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Description:
    This Function drops the tables using schemas from drop_table_queries.
    
    Arguments:
    cur - cursor object
    con - connection object
    
    Return:
    None
    """
    
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description:
    This Function creates the tables using schemas from create_table_queries.
    
    Arguments:
    cur - cursor object
    con - connection object
    
    Return:
    None
    """
    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
    This Function drops and creates the tables.
    
    Arguments:
    None
    
    Return:
    None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()