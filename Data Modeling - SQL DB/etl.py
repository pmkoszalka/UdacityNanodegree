#!/usr/bin/env python
# coding: utf-8

# # ETL process

# ### The purpose of this notebook is to develop the ETL process for each of the tables and prepare data for insertion process

# In[1]:


# imports necessary libraries

import os
import zipfile
import numpy as np
import pandas as pd


# In[2]:


# if folder data was not present in the CWD, it is being extracted from data.zip

mypath = os.getcwd()
dirpath, dirnames, filenames = next(os.walk(mypath))
if 'data' not in dirnames:
    with zipfile.ZipFile('data.zip', 'r') as zip_ref:
        zip_ref.extractall('')
else:
    print('Folder data is in your CWD!')


# In[3]:


# removes empty folders that were presnet in data folder and stores paths to files in paths list

paths = []
for (dirpath, dirnames, filenames) in os.walk(mypath):
    if not (dirnames or filenames):
        os.rmdir(dirpath)
    if any(True for file in filenames if file.endswith('json')):
        for file in filenames:
            paths.append(os.path.join(dirpath, file))


# In[4]:


# seperates paths to files into two categories: songs and logs

songs = []
logs = []
for file in paths:
    if 'song' in file:
        songs.append(file)
    else:
        logs.append(file)


# In[5]:


# creates logs df

logs_df = pd.read_json(logs[0], lines=True)
for log in logs[1:]:
    try:
        logs_df = pd.concat([logs_df, pd.read_json(log, lines=True)])
    except:
        pass


# In[6]:


# creates songs df

songs_df = pd.read_json(songs[0], lines=True)
for song in songs[1:]:
    songs_df = pd.concat([songs_df, pd.read_json(song, lines=True)])


# In[7]:


# creates users df, cleans data and stores it in a list of tuples form

users = logs_df[['userId','firstName', 'lastName', 'gender', 'level']].copy()
users['userId'] = users['userId'].astype(str)
users.drop_duplicates(inplace=True, subset='userId', keep="first")
users.dropna(inplace=True)
users['userId'] = users['userId'].astype(int)
users_data = [tuple(row.values) for idx,row in users.iterrows()]


# In[8]:


# creates songs df, cleans data and stores it in a list of tuples form

songs = songs_df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
songs.drop_duplicates(inplace=True, subset='song_id', keep="first")
songs_data = [tuple(row.values) for idx,row in songs.iterrows()]


# In[9]:


# creates artists df, cleans data and stores it in a list of tuples form

artists = songs_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
artists.drop_duplicates(inplace=True)
artists_data = [tuple(row.values) for idx,row in artists.iterrows()]


# In[10]:


# creates time df, cleans data and stores it in a list of tuples form

time = pd.DataFrame(pd.to_datetime(logs_df['ts'], unit='ms'))
time['hour'] = time['ts'].dt.hour
time['day'] = time['ts'].dt.day
time['week'] = time['ts'].dt.isocalendar().week
time['month'] = time['ts'].dt.month
time['year'] = time['ts'].dt.year
time['weekday'] = time['ts'].dt.weekday
time.drop_duplicates(inplace=True)
time_data = [tuple(row.values) for idx,row in time.iterrows()]


# In[11]:


# creates songplays df, cleans data and stores it in a list of tuples form

z = pd.merge(how ='outer', left=logs_df, right=songs_df, left_on=['artist', 'length'], right_on=['artist_name','duration'])
songplays=z[['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']].copy()

songplays['ts'] = pd.to_datetime(songplays['ts'], unit='ms')
songplays['songplay_id']= [num for num in range(len(songplays))]
songplays = songplays[['songplay_id', 'ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']]

idx_list = songplays[songplays['userId']==''].index
songplays.drop(idx_list, inplace=True)

idx_list = songplays['ts'][songplays['ts'].isnull()==True].index
songplays.drop(idx_list, inplace=True)
songplays_data = [tuple(row.values) for idx,row in songplays.iterrows()]

