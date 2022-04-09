import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os

class FileProcessor:
    
    def save_to_file(dataframe, file_name):
        path = "assets/" + file_name

        try:
            table = pa.Table.from_pandas(dataframe)
            pq.write_table(table, path)
        except:
            path = None

        return path

    def save_to_s3(source_file_path, file_name):
        AWS_ACCESS_KEY_ID = 'AKIA6N2KWCM3XLAJNQVG'
        AWS_SECRETE_KEY_ID = '2GSiMeg/MgZysH8FatmtHOV5wfRqMvhc2sML20uF'

        s3 = boto3.resource('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRETE_KEY_ID)
        s3.Object('stori-bucket', file_name).put(Body=open(source_file_path, 'rb'))

        #FileProcessor.delete_file(source_file_path)

    def generate_filename(file_name):
        date = datetime.datetime.now()

        file_name = file_name + date.strftime("%d%m%Y%H%M%S%f") + ".parquet"

        return file_name

    def delete_file(file_path):
        delete = 'rm ' + file_path
        os.system(delete)