import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
"""Optimalization of Table Design on Redshift:

1. Sortkeys
- Each table have single sort key, but sort keys can be compound -> comprised of 1 to 400 columns
- Redshift stores data on disk in a Sort Key order
- If joins are performed, it is probably a good idea to include columns that are used to join in a sort key
- Knowing the queries that will be performed, might decide where to put a sortkey for example: date in select * from sth where date ...
- Best for using sort keys: queries that use 1st column as primary filter, can spped up joins and group by

2. Distribution style
Three types: Distribution Key (same key to same location), All (all data in all nodes), Even (evenly across the nodes)

- by default -> even distribution
- destribution key is comprised of only a single column

a) Distribution key: for large fact and dimension tables
b) Distribution all - medium dimension tables (1K-2M)
c) Distribution even - tables with no join or group by, samll dimension tables (<1000)

Resource: https://www.youtube.com/watch?v=4P37f9OfH_A&t=38s
"""

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist         VARCHAR(255),
        auth           VARCHAR(255),
        firstName      VARCHAR(255),
        gender         CHAR(1),
        itemInSession  INT,
        lastName       VARCHAR(255),
        length         FLOAT,
        level          VARCHAR(255),
        location       VARCHAR(255),
        method         VARCHAR(255),
        page           VARCHAR(255),
        registration   VARCHAR(255),
        sessionId      INT,
        song           VARCHAR(255),
        status         INT,
        ts             NUMERIC,
        userAgent      VARCHAR(255),
        userId         INT
);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id         VARCHAR(255),
        artist_latitude   FLOAT,
        artist_location   VARCHAR(255),
        artist_longitude  FLOAT,
        artist_name       VARCHAR(255),
        duration          FLOAT,
        num_songs         INT,
        song_id           VARCHAR(255),
        song_title        VARCHAR(255),
        year              INT
);
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id       INT IDENTITY(0,1) SORTKEY NOT NULL, 
        start_time        TIMESTAMP, 
        user_id           INT, 
        level             VARCHAR(255),
        song_id           VARCHAR(255), 
        artist_id         VARCHAR(255), 
        session_id        INT, 
        location          TEXT, 
        user_agent        TEXT,
        PRIMARY KEY       (songplay_id),
        FOREIGN KEY       (user_id) references users(user_id),
        FOREIGN KEY       (song_id) references song(song_id),
        FOREIGN KEY       (artist_id) references artist(artist_id)
);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
       user_id             INT NOT NULL,
       first_name          VARCHAR(255),
       last_name           VARCHAR(255),
       gender              VARCHAR(255),
       level               VARCHAR(255),
       PRIMARY KEY         (user_id)
 );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id            VARCHAR(255) SORTKEY NOT NULL, 
        title              VARCHAR(255) NOT NULL, 
        artist_id          VARCHAR(255), 
        year               INT, 
        duration           DECIMAL,
        PRIMARY KEY        (song_id)
);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id         VARCHAR(255) SORTKEY NOT NULL, 
        name              VARCHAR(255) NOT NULL, 
        location          VARCHAR(255),
        latitude          DECIMAL, 
        longitude         DECIMAL,
        PRIMARY KEY       (artist_id)
);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time       TIMESTAMP SORTKEY NOT NULL, 
        hour             INT, 
        day              INT,
        week             INT, 
        month            INT,
        year             INT, 
        weekday          INT
);
""")

# STAGING TABLES
staging_events_copy = """
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON {}
    """.format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = """
    COPY staging_songs
    from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    ACCEPTINVCHARS
    format as JSON 'auto';
    """.format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplay (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)
(    
    SELECT DISTINCT 
        timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second', 
        se.userId, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    FROM staging_events AS se INNER JOIN staging_songs AS ss
    ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
); 
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level) WITH uniq_staging_events AS (
  SELECT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level,
    ROW_NUMBER() OVER(
      PARTITION BY userId 
      ORDER BY 
        ts DESC
    ) AS rank 
  FROM 
    staging_events 
  WHERE 
    userId IS NOT NULL AND page = 'NextSong'
)
SELECT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM 
  uniq_staging_events 
WHERE 
  rank = 1;
""")

song_table_insert = ("""
    INSERT INTO song (
          song_id, 
          title, 
          artist_id, 
          year, 
          duration
) 
(
    SELECT DISTINCT
        song_id, 
        song_title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs
    WHERE song_title IS NOT NULL
);
""")

artist_table_insert = ("""
    INSERT INTO artist (
      artist_id, 
      name, 
      location, 
      latitude, 
      longitude
) 
(
    SELECT DISTINCT
       artist_id, 
       artist_name, 
       artist_location, 
       artist_latitude, 
       artist_longitude
    FROM staging_songs  
);
    
""")

time_table_insert = ("""
    INSERT INTO time (
          start_time, 
          hour, 
          day, 
          week, 
          month, 
          year, 
          weekday
) 
(
    SELECT DISTINCT start_time,
        EXTRACT (HOUR FROM start_time) as hour,
        EXTRACT (DAY FROM start_time) as day,
        EXTRACT (WEEK FROM start_time) as week,
        EXTRACT (MONTH FROM start_time) as month,
        EXTRACT (YEAR FROM start_time) as year,
        EXTRACT (WEEKDAY FROM start_time) as weekday
     FROM songplay
);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
