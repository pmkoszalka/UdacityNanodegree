import configparser
import psycopg2
from sql_queries import create_table_queries, drop_query, table_names, copy_query
import re
from dbconnection import DBConnection
import logging
import logging_config

config = configparser.ConfigParser()
config.read('config.cfg')

BUCKET = config.get("S3","BUCKET")
IAM_ROLE = config.get("IAM_ROLE","ARN")

class Redshift(DBConnection):
    """Performs actions with database on Redshift"""
    
    def __init__(self):
        super().__init__()
        
    def drop_tables(self):
        """Drops tables using schemas from drop_table_queries"""
        
        for name in table_names:
            query = drop_query.format(name)
            logging.info(f"Table {name} has been dropped!")
            
            self.cur.execute(query)
            self.conn.commit()

    def create_tables(self):
        """Creates tables using schemas from create_table_queries"""
        
        for query, table_name in zip(create_table_queries, table_names):
            logging.info(f"Table {table_name} has been created!")
            
            self.cur.execute(query)
            self.conn.commit()

    def copy_tables(self):
        """Creates tables for stagging from copy_table_queries"""
        
        for name in table_names:
            query = copy_query(name, BUCKET, IAM_ROLE)
            logging.info(f"Data to table {name} has been inserted!")
            
            self.cur.execute(query)
            self.conn.commit()


def main():
    """Pipeline for droping and creating tables and then inserting data into them"""
    
    with Redshift() as r:
        r.drop_tables()
        r.create_tables()
        r.copy_tables()

if __name__ == "__main__":
    main()