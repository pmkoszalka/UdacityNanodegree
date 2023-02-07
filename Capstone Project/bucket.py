import pandas as pd
import boto3
import all_df
import configparser
import logging
import logging_config
from botocore.exceptions import ClientError
from io import StringIO
from typing import Optional

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

BUCKET = config.get("S3","BUCKET")

class Bucket:
    """Performs actions with AWS S3 Bucket"""
    
    def __init__(self):
        self.key = config.get('AWS','KEY')
        self.secret = config.get('AWS','SECRET')
        
        self.s3 = boto3.client('s3', 
                    region_name='us-east-1', 
                    aws_access_key_id=self.key, 
                    aws_secret_access_key=self.secret)

        self.s3_resource = boto3.resource('s3', aws_access_key_id=self.key, aws_secret_access_key=self.secret)
        
    def create_bucket(self, bucket_name: str, region: Optional[str] = None) -> None:
        """Create an S3 bucket in a specified region"""
        
        try:
            if region is None:
                self.s3.create_bucket(Bucket=bucket_name)
                logging.info(f"Bucket {bucket_name} has been created!")
            else:
                location = {'LocationConstraint': region}
                self.s3.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    
    def upload_to_s3(self, bucket: str, list_dfs: list) -> None:
        """Uploads DataFrames to s3 bucket"""
        
        for df in list_dfs:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            name = df.name
            self.s3_resource.Object(bucket, name + '.csv').put(Body=csv_buffer.getvalue())
            logging.info(f"File {name} has been uploaded to bucket {bucket}")
            
def main() -> None:
    """Pipeline for creating bucket and uploading files to it"""
    
    list_df = all_df.main()
    b = Bucket()
    b.create_bucket(BUCKET)
    b.upload_to_s3(BUCKET, list_df)
    
if __name__ == "__main__":
    main()