import pandas as pd
import numpy as np
from typing import List, Tuple
import immigration
import airports
import demographics
from io import StringIO
import boto3
import logging
import logging_config

class All:
    """Perfroms transformations on all DataFrames"""
    
    def __init__(self):
        self.airports = airports.main()
        self.immigration = immigration.main()
        self.demographics = demographics.main()
        self.states = None
        self.cities = None
        self.passengers = None
        self.flights = None
        
        # attributes for index mapping
        self._city_state = None
        self._mapper_states = None
        
        # list of DataFrames for further etl
        self.dataframes = []
        
    def get_states(self) -> None:
        """Produces states DataFrame"""
        
        # gathers USA states information from website
        states = pd.read_html('https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971')[0]
        
        # gathers necessary information, renames and sorts columns
        states = states[['State', 'Alpha code']]
        self.states = states.rename(columns={'State':'state', 'Alpha code': 'state_code'}).sort_values('state')
        
        # creates a map of indexes of states
        mapper_states = {}
        for i, r in self.states.iterrows():
            mapper_states[r[1]] = i
        self._mapper_states = mapper_states
        
        # changes the order of the columns
        self.states = self.states[['state_code', 'state']]
        
        # sets the name for the DataFrame
        self.states.name = 'states'
        
        # appends table to list
        self.dataframes.append(self.states)
        logging.info('Table states has been created!')
        
    def get_cities(self) -> None:
        """Produces cities DataFrame ready to be loaded to Redshift"""
        
        # gathers cities from all the DataFrames
        a_city = self.airports[['city', 'state_code']]
        i_city = self.immigration[['city', 'state_code']]
        d_city = self.demographics[['city', 'state_code']]
        cities = pd.concat([a_city, i_city, d_city]).drop_duplicates().reset_index(drop=True)
        
        # maps state index
        cities['state_id'] = cities['state_code'].apply(lambda x:  self._mapper_states[x] if x in  self._mapper_states.keys() else np.nan)
        
        # drops tables where state index was not applied
        cities.dropna(inplace=True)
        self.cities = cities[['state_id', 'city']].sort_values(by='city').reset_index(drop=True)
        self.cities['state_id'] = self.cities['state_id'].astype(int)
        
        # creates city_state for further city index mapping
        self._city_state = pd.merge(self.cities, self.states, left_on='state_id', right_on=self.states.index).reset_index()
        
        # sets the name for the DataFrame
        self.cities.name = 'cities'
        
        # appends table to list
        self.dataframes.append(self.cities)
        logging.info('Table cities has been created!')
        
    def get_immigration(self) -> None:
        """Produces immigration DataFrame ready to be loaded to Redshift"""
        
        # filters immigration DataFrame to US
        self.immigration['USA'] = self.immigration['state_code'].apply(lambda x: True if x in self._mapper_states.keys() else False)
        self.immigration = self.immigration[self.immigration['USA']==True]
        self.immigration.drop('USA', axis=1, inplace=True)
        
        # maps city index
        self.immigration = pd.merge(self.immigration, self._city_state[['city', 'state_code', 'index']], on=['city', 'state_code'])
        
        # renames and drops columns
        self.immigration = self.immigration.rename(columns={'index':'city_id'}).drop(['state_code', 'city'], axis=1)
        
    def get_airports(self) -> None:
        """Produces airports DataFrame ready to be loaded to Redshift"""
        
        # maps city index
        self.airports = pd.merge(self.airports, self._city_state[['city', 'state_code', 'index']], on=['city', 'state_code'])
        # renames and drops columns
        self.airports = self.airports.rename(columns={'index':'city_id'}).drop(['state_code', 'city'], axis=1)
        
        # changes the order of the columns
        self.airports = self.airports[['city_id', 'ident', 'airport_name', 'type', 'elevation_ft', 'coordinates']]
        
        # sets the name for the DataFrame
        self.airports.name = 'airports'
        
        # appends table to list
        self.dataframes.append(self.airports)
        logging.info('Table airports has been created!')
        
    def get_demographics(self) -> None:
        """Produces demographics DataFrame ready to be loaded to Redshift"""
        
        # maps city index
        self.demographics = pd.merge(self.demographics, self._city_state[['city', 'state_code', 'index']], on=['city', 'state_code'])
        # renames and drops columns
        self.demographics = self.demographics.rename(columns={'index':'city_id'}).drop(['state_code', 'city', 'state'], axis=1)
        # changes the order of the columns
        self.demographics = self.demographics[['city_id', 'median_age', 'male_population', 'female_population',
'total_population', 'number_of_veterans', 'foreign_born','average_household_size', 'american_indian_and_alaska_native', 'asian','black_or_african_american', 'hispanic_or_latino', 'white']]
        
        # sets the name for the DataFrame
        self.demographics.name = 'demographics'
        
        # appends table to list
        self.dataframes.append(self.demographics)
        logging.info('Table demographics has been created!')
        
    def get_passengers(self) -> None:
        """Produces passengers DataFrame ready to be loaded to Redshift"""
        
        # defines passengers DataFrame
        self.passengers = self.immigration[['id', 'gender', 'birth_year', 'respondent_age', 'visa_type', 'visa_code', 'admission_number', 'date_until_allowed_stay']].reset_index().rename(columns={'index': 'flight_id', 'id': 'passenger_number', 'respondent_age': 'passenger_age'})
        
        # sets the name for the DataFrame
        self.passengers.name = 'passengers'
        
        # appends table to list
        self.dataframes.append(self.passengers)
        logging.info('Table passengers has been created!')
        
    def get_flights(self) -> None:
        """Produces flights DataFrame ready to be loaded to Redshift"""
        
        # defines flights DataFrame
        self.flights = self.immigration[['city_id', 'origin_country', 'arrival_date_usa', 'departure_date_usa', 'mode', 'arrival_flag', 'departure_flag', 'airline', 'flight_number']]
        
        # sets the name for the DataFrame
        self.flights.name = 'flights'
        
        # appends table to list
        self.dataframes.append(self.flights)
        logging.info('Table flights has been created!')
    
def main() -> List[pd.DataFrame]:
    """Pipeline for producing DataFrames and its names to be loaded to Redshift"""
    
    a = All()
    a.get_states()
    a.get_cities()
    a.get_immigration()
    a.get_airports()
    a.get_demographics()
    a.get_passengers()
    a.get_flights()
    return a.dataframes
    
    
if __name__ == "__main__":
    main()