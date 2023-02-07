import pandas as pd

PATH = 'data/us-cities-demographics.csv'

class Demographics:
    """Performs transformations on demographic dataframe"""
    
    def __init__(self, path: str):
        self.path = path
        self.df = pd.read_csv(self.path, sep=';')
        
    def pivot_columns(self) -> None:
        """Pivots data regarding minorities to be visible in seperate columns"""
        
        self.df = self.df.pivot_table('Count', list(self.df.columns[:-2]), 'Race').reset_index()
        self.df.drop_duplicates(inplace=True)
    
    def lower_column_names(self) -> None:
        """Lowers columns names"""
        
        self.df.columns = [x.lower().replace(' ','_').replace('-', '_') for x in self.df.columns]
        
    def remove_nans(self) -> None:
        """Drops rows with NaNs"""
        
        self.df.dropna(inplace=True)
    
    def change_columns_type_to_int(self) -> None:
        """Changes columns types from float to int"""
        
        for col in self.df.columns:
            # two columns are excluded: median_age and average_household_size
            if col in ['median_age', 'average_household_size']:
                continue
            if self.df[col].dtype == 'float64':
                try:
                    self.df[col] = self.df[col].astype(int)
                except Exception:
                    continue
    

def main() -> pd.DataFrame:
    """Pipeline for demographic DataFrame"""
    
    d = Demographics(PATH)
    d.pivot_columns()
    d.lower_column_names()
    d.remove_nans()
    d.change_columns_type_to_int()
    return d.df

if __name__ == "__main__":
    main()