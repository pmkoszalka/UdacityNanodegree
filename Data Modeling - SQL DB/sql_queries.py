#!/usr/bin/env python
# coding: utf-8

# # SQL Queries
# ## Workbook contains class that contcts to Postgre and performs simple actions involving inputation of data from retreived by etl.py

# ## The task is to create the following tables and then input provided data:
# 
# ### Fact Table
# 
# 1) songplays - records in log data associated with song plays i.e. records with page NextSong
# - columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
# 
# ### Dimension Tables
# 
# 1) users - users in the app
# - columns: user_id, first_name, last_name, gender, level
# 
# 2) songs - songs in music database
# - columns: song_id, title, artist_id, year, duration
# 
# 3) artists - artists in music database
# - columns: artist_id, name, location, latitude, longitude
# 
# 4) time - timestamps of records in songplays broken down into specific units
# - columns: start_time, hour, day, week, month, year, weekday

# In[1]:


import psycopg2
import re
from etl import *


# In[2]:


class Postgres:
    '''Connects to Postres and performs simple actions'''
    
    _tables = {}
    
    def __init__(self, host = 'localhost', port='5432', user='postgres', password='student'):
        '''Creates attributes for connecting to database and performin queries'''
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.con = None
        self.cur = None
        
    def __enter__(self):
        '''Connects to Postgre upon using contex manager'''
        
        self.con = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password)
        self.cur = self.con.cursor()
        self.con.set_session(autocommit=True)
        return self

    def create_database(self, db='sparkifydb'):
        '''Drops database if exists and creates new one'''
        
        self.cur.execute("DROP DATABASE IF EXISTS " + db + ';')
        self.cur.execute('CREATE DATABASE ' + db + ';')
    
    def connect_to_db(self, db='sparkifydb'):
        '''Connects to database'''
        
        self.con = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=db)
        self.cur = self.con.cursor()
        self.con.set_session(autocommit=True)
        print('You have been conected to database: '+ db)
        
    def list_tables(self):
        '''Lists tables in database'''
        
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
        row = self.cur.fetchone()
        tables = ''
        while row:
            tables = tables + str(row)
            row = self.cur.fetchone()
        tables = re.sub(r"[\(\)]",'',tables)[:-1]
        print('At your current DB you have the following tables: '+tables)
        
    def create_table(self, queries):
        '''Creates table'''
        
        assert type(queries)==list, 'List of queries should be provided; The queries should start with CREATE TABLE'
        for query in queries:
            
            assert 'CREATE TABLE' in query, 'The query should start with CREATE TABLE'
            
            name = query.split('CREATE TABLE ')[1].split(' ')[0]
            self.cur.execute('DROP TABLE IF EXISTS '+name+';')
            self.cur.execute(query)
            
            columns = query.split('(')[1].split(')')[0].split(',')
            columns = [col.split( )[0].strip() for col in columns]
            Postgres._tables[name] = columns
            
            print('Table '+name+' created successfully!')
    
    
    def insert_data(self, table_name, data):
        '''Inserts data to table'''
        
        assert table_name in Postgres._tables, 'Create a table beforehand!'
        assert type(data)== list, 'This should be a list of tuples!'
        
        for d in data:

            insert_query = 'INSERT INTO '+ table_name + ' (' + ', '.join(Postgres._tables[table_name])+') VALUES '+                         '(' + '%s, ' * (len(Postgres._tables[table_name])-1)+'%s) ON CONFLICT                         ('+Postgres._tables[table_name][0]+') DO NOTHING;'
            
            self.cur.execute(insert_query, d)
            
    def print_columns(self, table_name):
        '''Prints column names of a table'''
        
        assert table_name in Postgres._tables, 'Create a table beforehand!'
        self.cur.execute('Select * FROM '+table_name+' LIMIT 0')
        colnames = [desc[0] for desc in self.cur.description]
        print(colnames)
    
            
    def print_data(self, table_name):
        '''Prints all data of a table'''
        
        self.cur.execute('SELECT * FROM '+table_name+';')
        row = self.cur.fetchone()
        
        print(table_name)
        print(', '.join(Postgres._tables[table_name]))
        while row:
            print(row)
            row = self.cur.fetchone()
        
    def execute_query(self, query):
        '''Executes a custom query'''
        
        self.cur.execute(query)
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        '''Closes the cursor and conection via contex manager'''
        
        self.cur.close()
        self.con.close()
        print('Conection closed!')


# In[3]:


songplays = '''CREATE TABLE songplays 
                (songplay_id INT PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, level VARCHAR NOT NULL,
                song_id VARCHAR NOT NULL, artist_id VARCHAR NOT NULL, session_id INT NOT NULL, 
                location VARCHAR NOT NULL, user_agent VARCHAR NOT NULL);'''
users = '''CREATE TABLE users 
                (user_id INT PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);'''
songs = '''CREATE TABLE songs (song_id VARCHAR PRIMARY KEY, title VARCHAR NOT NULL, artist_id VARCHAR NOT NULL, 
year INT NOT NULL, duration INT NOT NULL)'''
artists = '''CREATE TABLE artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, location VARCHAR NOT NULL,
latitude FLOAT NOT NULL, longitude FLOAT NOT NULL)'''
time = '''CREATE TABLE time (start_time TIMESTAMP PRIMARY KEY, hour INT, day INT, week INT, month INT,
                year INT, weekday INT)'''

queries = [songplays, users, songs, artists, time]


# In[4]:


# created for the sake of testing
songplay_table_create = songplays
user_table_create = users
song_table_create = songs
artist_table_create =artists
time_table_create = time

songplay_table_insert = """INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, 
location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO NOTHING;"""

user_table_insert = """INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) DO NOTHING;"""

song_table_insert = """INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (song_id) DO NOTHING;"""

artist_table_insert = """'INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id) DO NOTHING;';"""


time_table_insert = '''INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;'''


# In[5]:


if __name__ =='__main__':
    with Postgres() as p:
        p.create_database()
        p.connect_to_db()
        print(p._tables)
        p.create_table(queries)
        p.list_tables()
        for name in ['songplays', 'users', 'songs', 'artists', 'time']:
            p.print_columns(name)
            print('-------------')
        p.insert_data('users', users_data)
        p.print_data('users')
        print('------------------')
        p.insert_data('songs', songs_data)
        p.print_data('songs')
        print('------------------')
        p.insert_data('artists', artists_data)
        p.print_data('artists')
        print('------------------')
        p.insert_data('time', time_data)
        p.print_data('time')
        print('------------------')
        p.insert_data('songplays', songplays_data)
        p.print_data('songplays')

