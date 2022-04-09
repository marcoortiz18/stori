import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime
from Connector import Connector
from FileProcessor import FileProcessor

class Mongo:
    
    def start(self):
        response = True
        mongo = Mongo()

        try:
            # This should be in secrets, but for this app I leave it hard coded
            host = "mongodb+srv://stori_mongo:Stori_123@cluster0.stwqn.mongodb.net/test"
            database = "stori"

            # Connect with the database
            connector = Connector(host, database, None, None)
            connection = connector.connect_to_mongo()
            collection = connection.account

            # Set the dataframe with source from mongo
            data = pd.DataFrame(list(collection.find()))
            data = mongo.build(data)

            # Generate the file, save local & save into S3
            file_name = FileProcessor.generate_filename("mongo_")
            source_file_path = FileProcessor.save_to_file(data, file_name)
            FileProcessor.save_to_s3(source_file_path, file_name)
        except:
            response = False

        return response

    # Build the correct structure to match with redshift
    def build(self, data):
        
        data["id"] = data['id'].fillna("").astype('string')
        data["details"] = data['details'].fillna("").astype('string')
        data["ticker"] = data['ticker'].fillna("").astype('string')
        data["ticket"] = data['ticket'].fillna("").astype('string')
        data["price"] = data['price'].fillna(0).astype('int64')
        data["shares"] = data['shares'].fillna(0).astype('int64')
        data["date"] = data['time'][0]["date"]
        data["date"] = data['date'].fillna("").astype('string')
        data.pop('_id')
        data.pop('time')

        return data  

# starting the process
mongo_pipeline = Mongo()
mongo_pipeline.start()