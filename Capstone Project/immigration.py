from typing import Dict, List, Tuple
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime

PATH = 'data/sas_data/*.parquet'
COLUMN_LIST = ['biryear', 'cicid', 'i94res', 'arrdate', 'i94bir', 'i94visa', 'admnum']
DESCRIPTON = 'data/I94_SAS_Labels_Descriptions.SAS'
COLUMN_NAMES_MAPPER = {'cicid': 'id', 'i94res': 'origin_country', 'arrdate': 'arrival_date_usa', 'i94mode': 'mode','depdate': 'departure_date_usa', 'i94bir': 'respondent_age', 'i94visa': 'visa_code','entdepa': 'arrival_flag', 'entdepd': 'departure_flag', 'biryear': 'birth_year', 'dtaddto': 'date_until_allowed_stay', 'admnum': 'admission_number', 'fltno': 'flight_number', 'visatype':'visa_type'}
COLUMNS_TO_DROP = ['occup', 'entdepu', 'visapost', 'insnum', 'i94mon', 'i94yr', 'count', 'matflag', 'i94addr', 'i94cit', 'dtadfile', 'i94port']

class Immigration:
    """Performs transformations on immigration dataframe"""
    
    def __init__(self, path: str):
        self.path = path
        self.df = SparkSession.builder.appName("PySpark Read Parquet").getOrCreate().read.parquet(self.path).toPandas()
        
        # stores source file's description for extractions
        with open(DESCRIPTON, 'r') as f:
            self.desc_lines = f.readlines()

    def change_columns_types(self, columns_list: List[str], data_type: str) -> None:
        """Changes columns types to int"""
        
        for column in columns_list:
            self.df[column] = self.df[column].astype(data_type)

    def extract_country_codes(self) -> None:
        """Extracts and fills country codes"""
        
        # extracts country codes from description
        codes_country = {}
        for line in self.desc_lines[9:298]:
            code = line.strip().split(' = ')[0].strip()
            code = int(code)
            country = line.strip().split(' = ')[1].replace("'", "").strip()
            codes_country[code] = country
        
        # fills country codes
        self.df['i94res'] = self.df['i94res'].apply(lambda x: codes_country[x].title() if x in codes_country else np.nan)

    def extract_columns_modes(self) -> None:
        """Extracts and fills modes"""
        
        # extracts modes
        mode_mapper = {}
        for line in self.desc_lines[972:976]:
            line_splited = line.split(' = ')

            # taking care of numbers
            number = line_splited[0]
            number = number.replace('\n', '')

            # taking care of mode
            mode = line_splited[1]
            mode = mode.replace("'", "").replace(";", "").replace("\n", "").strip()

            mode_mapper[float(number)] = mode
            
        # fills modes
        self.df['i94mode'] = self.df['i94mode'].apply(lambda x: mode_mapper[x] if x in mode_mapper else np.nan)
        
    def extract_columns_port(self) -> None:
        """Extracts and fills ports"""
        
        # extracts ports from description
        port_mapper = {}
        for line in self.desc_lines[302:893]:
            line_splited = line.split('=')
            try:
                # taking care of port
                port = line_splited[0]
                port = port.strip().replace("'", "")

                # taking care of city and city_code
                city_code = line_splited[1]
                city_code = city_code.replace("'", "").replace(";", "").replace("\n", "").strip()

                city_code_list = city_code.split(',')
                city = city_code_list[0].title()
                code = city_code_list[1].strip()

                port_mapper[port] = (city, code)
            except Exception:
                pass
        
        # fills ports
        self.df['city'] = self.df['i94port'].apply(lambda x: port_mapper[x][0] if x in port_mapper else np.nan)
        self.df['state_code'] = self.df['i94port'].apply(lambda x: port_mapper[x][1] if x in port_mapper else np.nan)

    def drop_nans_cols(self, columns_to_drop: List[str]) -> None:
        """Drops columns"""
        
        self.df.drop(columns_to_drop, axis=1, inplace=True)
        
    def drop_nans_rows(self) -> None:
        """Drops rows with NaNs"""
    
        self.df.dropna(inplace=True)
        
    @staticmethod
    def _dtaddto_to_timestamp(x: str, pattern: str) -> pd.Timestamp:
        """Converts string date format to timestamp"""
        
        # if immigrant can stay parmanently, their date_untill_allowed_stay is max
        if x=='D/S':
            return pd.Timestamp.max
        try:
            return pd.Timestamp(datetime.strptime(str(x),pattern))
        except Exception as e:
            return np.nan
    
    def change_columns_timestamp(self) -> None:
        """Transforms columns' data type to timestamp"""
        
        self.df['arrdate'] = self.df['arrdate'].apply(lambda x: pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1'))
        self.df['depdate'] = self.df['depdate'].apply(lambda x: pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1'))
        self.df['dtaddto'] = self.df['dtaddto'].apply(lambda x: Immigration._dtaddto_to_timestamp(x, '%m%d%Y'))
        
    def rename_columns(self, column_names_mapper: Dict[str, str]) -> None:
        """Renames unused columns"""
        
        self.df.rename(columns=column_names_mapper, inplace = True)
    
    def reset_index(self):
        """Resets index"""
        
        self.df.reset_index(drop=True)
        
def main() -> pd.DataFrame:
    """Pipeline for immigration DataFrame"""
    
    immigration = Immigration(PATH)
    immigration.change_columns_types(COLUMN_LIST, 'int')
    immigration.extract_country_codes()
    immigration.extract_columns_modes()
    immigration.extract_columns_port()
    immigration.drop_nans_cols(COLUMNS_TO_DROP)
    immigration.drop_nans_rows()
    immigration.change_columns_timestamp()
    immigration.drop_nans_rows()
    immigration.rename_columns(COLUMN_NAMES_MAPPER)
    immigration.reset_index()
    return immigration.df

if __name__ == "__main__":
    main()
