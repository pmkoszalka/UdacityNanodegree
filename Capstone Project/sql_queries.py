import configparser
import re

config = configparser.ConfigParser()
config.read('config.cfg')

# drop table query
drop_query = "DROP TABLE IF EXISTS {} CASCADE;"

# create table queries
states_table_create= ("""
    CREATE TABLE IF NOT EXISTS states (
        id                INT SORTKEY NOT NULL, 
        state_code        VARCHAR(255) NOT NULL,
        state             VARCHAR(255) NOT NULL, 
        PRIMARY KEY       (id)
);
""")

cities_table_create = ("""
    CREATE TABLE IF NOT EXISTS cities (
        id                INT SORTKEY NOT NULL,
        state_id          INT NOT NULL,
        city              VARCHAR(255) NOT NULL,
        PRIMARY KEY       (id),
        FOREIGN KEY       (state_id) references states(id)
);
""")

airports_table_create = ("""
    CREATE TABLE IF NOT EXISTS airports (
        id                INT SORTKEY NOT NULL,
        city_id           INT NOT NULL,
        ident             VARCHAR(255) NOT NULL,
        airport_name      VARCHAR(255) NOT NULL, 
        type              VARCHAR(255) NOT NULL, 
        elevation_ft      INT NOT NULL,
        coordinates       VARCHAR(255) NOT NULL, 
        PRIMARY KEY       (id),
        FOREIGN KEY       (city_id) references cities(id)
);
""")

demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS demographics (
        id                                   INT SORTKEY NOT NULL,
        city_id                              INT NOT NULL, 
        median_age                           FLOAT NOT NULL, 
        male_population                      INT NOT NULL, 
        female_population                    INT NOT NULL, 
        total_population                     INT NOT NULL, 
        number_of_veterans                   INT NOT NULL, 
        foreign_born                         INT NOT NULL, 
        average_household_size               FLOAT NOT NULL, 
        american_indian_and_alaska_native    INT NOT NULL, 
        asian                                INT NOT NULL, 
        black_or_african_american            INT NOT NULL, 
        hispanic_or_latino                   INT NOT NULL, 
        white                                INT NOT NULL, 
        PRIMARY KEY                          (id),
        FOREIGN KEY                          (city_id) references cities(id)
);
""")


flights_table_create = ("""
    CREATE TABLE IF NOT EXISTS flights (
        id                    INT SORTKEY NOT NULL, 
        city_id               INT NOT NULL,
        origin_country        VARCHAR(255) NOT NULL, 
        arrival_date_usa      TIMESTAMP NOT NULL,
        departure_date_usa    TIMESTAMP NOT NULL,
        mode                  VARCHAR(255) NOT NULL, 
        arrival_flag          VARCHAR(255) NOT NULL, 
        departure_flag        VARCHAR(255) NOT NULL, 
        airline               VARCHAR(255) NOT NULL, 
        flight_number         VARCHAR(255) NOT NULL, 
        PRIMARY KEY           (id),
        FOREIGN KEY           (city_id) references cities(id)
);
""")

passengers_table_create = ("""
    CREATE TABLE IF NOT EXISTS passengers (
        id                          INT SORTKEY NOT NULL, 
        flight_id                   INT NOT NULL, 
        passenger_number            INT NOT NULL,
        gender                      VARCHAR(255) NOT NULL,
        birth_year                  INT NOT NULL, 
        passenger_age               INT NOT NULL, 
        visa_type                   VARCHAR(255) NOT NULL,
        visa_code                   INT NOT NULL, 
        admission_number            BIGINT NOT NULL, 
        date_until_allowed_stay     TIMESTAMP NOT NULL,
        PRIMARY KEY                 (id),
        FOREIGN KEY                 (flight_id) references flights(id)
);
""")

# copy table function
def copy_query(table_name: str, bucket_name: str, iam_role: str) -> str:
    """Provides copy_query"""

    query = """
        COPY {}
        FROM 's3://{}/{}.csv'
        CREDENTIALS 'aws_iam_role={}'
        CSV
        IGNOREHEADER 1;
        """.format(table_name, bucket_name, table_name, iam_role)
    return query

# for dropping and creating tables in AWS Redhsift and for coping the data 
create_table_queries = [states_table_create, cities_table_create, demographics_table_create, flights_table_create, passengers_table_create, airports_table_create]
table_names = [re.findall('EXISTS ([^ \(]+)', phrase)[0] for phrase in create_table_queries]