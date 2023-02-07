import pandas as pd
from typing import Dict

PATH = 'data/airport-codes.csv'
RENAME_MAPPER = {'municipality': 'city', 'name': 'airport_name'}
COLUMNS_TO_DROP = ['gps_code', 'iata_code', 'local_code', 'continent', 'iso_country', 'iso_region']

class Airports:
    """Performs transformations on airports dataframe"""
    
    def __init__(self, path: str) -> None:
        self.path = path
        self.df = pd.read_csv(self.path)
    
    def extract_state_codes(self) -> None:
        """Extracts states codes"""
        
        self.df['state_code'] = self.df['iso_region'].apply(lambda x: x.split('-')[1])
    
    def filter_usa(self) -> None:
        """Filters the table to US only"""
        
        self.df = self.df[(self.df['iso_country']=='US') & (self.df['state_code']!='U')]
    
    def drop_nans(self, columns_to_drop) -> None:
        """Drops columns that are not used and NaNs"""
        
        self.df.drop(columns_to_drop, axis=1, inplace=True)
        self.df.dropna(inplace=True)
    
    def reset_index(self) -> None:
        """Produces initial airports table"""
        
        self.df = self.df.reset_index(drop=True)
    
    def rename_column(self, rename_mapper: Dict[str, str]) -> None:
        """Renames column"""
        
        self.df = self.df.rename(columns=rename_mapper)
        
    def change_column_type(self, column_name: str, column_type: type) -> None:
        """Changes column type"""
        
        self.df[column_name] = self.df[column_name].astype(column_type)
        
        
def main() -> pd.DataFrame:
    """Pipeline for airports DataFrame"""
    
    a = Airports(PATH)
    a.extract_state_codes()
    a.filter_usa()
    a.drop_nans(COLUMNS_TO_DROP)
    a.reset_index()
    a.rename_column(RENAME_MAPPER)
    a.change_column_type('elevation_ft', int)
    return a.df


if __name__ == "__main__":
    main()