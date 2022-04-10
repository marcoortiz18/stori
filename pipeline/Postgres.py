import pandas as pd

from Connector import Connector
from FileProcessor import FileProcessor
from Redshift import Redshift

class Postgres:
    
    def start(self):
        response = True
        postgres = Postgres()
        
        try:

            # Get all the secrets keys
            secret = Connector.get_secrets("dev/stori/pg")
            host = secret['host']
            database = secret['dbname']
            password = secret['password']
            user = secret['username']
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

            # Copy parquet file to resdhift
            Redshift.copy_to_redshift(file_name)
        
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