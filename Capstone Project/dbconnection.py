import pandas as pd
import numpy as np
import configparser
import psycopg2
from sql_queries import table_names
from typing import Optional, Type, TypeVar, Any
import logging
import logging_config

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

T = TypeVar("T")

class DBConnection:
    """Connects to Database"""
    
    def __init__(self):
        self.DWH_DB_USER = config.get("CLUSTER","DWH_DB_USER")
        self.DWH_DB_PASSWORD = config.get("CLUSTER","DWH_DB_PASSWORD")
        self.DWH_ENDPOINT = config.get("CLUSTER","DWH_ENDPOINT")
        self.DWH_PORT = config.get("CLUSTER","DWH_PORT")
        self.DWH_DB = config.get("CLUSTER","DWH_DB")
        
        self.conn = psycopg2.connect(
            host=self.DWH_ENDPOINT,
            port=self.DWH_PORT,
            dbname=self.DWH_DB,
            user=self.DWH_DB_USER,
            password=self.DWH_DB_PASSWORD
        )
        self.cur = self.conn.cursor()
        
    def __enter__(self) -> "DBConnection":
        """Allows to use the class with context manager"""
        
        logging.info(f"Connection to {self.DWH_DB} has been established!")
        return self
    
    def __exit__(self, ex_type: Optional[Type[T]], ex_value: Optional[T], ex_traceback: Optional[Any]) -> Optional[bool]:
        """Closes the conection to the database"""
        
        self.cur.close()
        self.conn.close()
        logging.info(f"Connection to {self.DWH_DB} has been closed!")