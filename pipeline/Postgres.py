import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from Connector import Connector
from FileProcessor import FileProcessor

class Postgres:
    
    def start(self):
        response = True
        postgres = Postgres()
        
        try:
            # This should be in secrets, but for this app I leave it hard coded
            host = "stori.ct6otmz7rrfi.us-east-1.rds.amazonaws.com"
            database = "stori_db"
            password = "Stori_123"
            user = "stori_user"
            query = 'select * from stori.account order by 1'

            # Connect with the database
            connector = Connector(host, database, user, password)
            connection = connector.connect_to_pg()
            
            # Set the dataframe with source from postgres
            data = pd.read_sql(query, connection)
            data = postgres.build(data)

            # Generate the file, save local & save into S3
            file_name = FileProcessor.generate_filename("pg_")
            source_file_path = FileProcessor.save_to_file(data, file_name)
            FileProcessor.save_to_s3(source_file_path, file_name)
        except:
            response = False

        return response

    # Build the correct structure to match with redshift
    def build(self, data):
        data["account_id"] = data['account_id'].fillna(0).astype('int32')
        data["account_no"] = data['account_no'].fillna(0).astype('int64')
        data["date"] = data['date'].astype('datetime64')
        data["transaction_details"] = data['transaction_details'].fillna(0).astype('string')
        data["chip_used"] = data['chip_used'].fillna("").astype('bool')
        data["value_date"] = data['value_date'].astype('datetime64')
        data["withdrawal_amt"] = data['withdrawal_amt'].fillna(0).astype('int64')
        data["deposit_amt"] = data['deposit_amt'].fillna(0).astype('int64')
        data["balance_amt"] = data['balance_amt'].fillna(0).astype('int64')

        return data

# starting the process
postgres_pipeline = Postgres()
postgres_pipeline.start()