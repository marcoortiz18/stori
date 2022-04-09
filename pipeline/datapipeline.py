import boto3
from isort import Config
import psycopg2
import csv
#import pyarrow as pa
#import pyarrow.parquet as pq
import time
import redshift_connector
import sys
import os
import datetime
from datetime import date
datetime_object = datetime.datetime.now()
print ("Start TimeStamp")
print ("---------------")
print(datetime_object)


db_opts = {
    'user': 'stori_user',
    'password': 'Stori_123',
    'host': 'stori.ct6otmz7rrfi.us-east-1.rds.amazonaws.com',
    'database': 'stori_db'
}

db = psycopg2.connect(**db_opts)
cur = db.cursor()

query = 'select * from stori.account order by 1'
csv_file_path = './assets/pg.csv'
 
try:
    cur.execute(query)
    rows = cur.fetchall()
finally:
    db.close()

# Continue only if there are rows returned.
if rows:
    # New empty list called 'result'. This will be written to a file.
    result = list()
 
    # The row name is the first entry for each entity in the description tuple.
    column_names = list()
    for i in cur.description:
        column_names.append(i[0])
 
    result.append(column_names)
    for row in rows:
        result.append(row)
 
    # Write result to file.
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in result:
            csvwriter.writerow(row)
else:
    sys.exit("No rows found for query: {}".format(query))

# Upload Generated CSV File to S3 Bucket
AWS_ACCESS_KEY_ID = 'AKIA6N2KWCM3XLAJNQVG'
AWS_SECRETE_KEY_ID = '2GSiMeg/MgZysH8FatmtHOV5wfRqMvhc2sML20uF'

s3 = boto3.resource('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRETE_KEY_ID)
bucket = s3.Bucket('stori-bucket')
s3.Object('stori-bucket', 'pg.csv').put(Body=open(csv_file_path, 'rb'))

#Obtaining the connection to RedShift
#con_redshift=psycopg2.connect(dbname= 'stori_db_redshift',
#  host='stori-redshift-cluster.csgkxigtifx1.us-east-1.redshift.amazonaws.com:5439/dev',port= '5439', user= 'stori_redshift_user', password= 'Stori_123')
 
con_redshift = redshift_connector.connect(
    host='stori-redshift-cluster.csgkxigtifx1.us-east-1.redshift.amazonaws.com',
    database='stori_db_redshift',
    user='stori_redshift_user',
    password='Stori_123'
)

#Copy Command as Variable
#copy_command="copy employee from 's3://mybucket-shadmha/my_csv_file.csv' credentials 'aws_iam_role=arn:aws:iam::775888:role/REDSHIFT' delimiter '|' region 'ap-southeast-2' ignoreheader 1 removequotes ;"
 
#Opening a cursor and run copy query
cur = con_redshift.cursor()
cur.execute("truncate table stori_account.account;")
#cur.execute(copy_command)
con_redshift.commit()
 
#Close the cursor and the connection
cur.close()
con_redshift.close()
 
# Remove the S3 bucket file and also the local file
DelLocalFile = 'aws s3 rm s3://mybucket-shadmha/my_csv_file.csv --quiet'
DelS3File = 'rm /home/centos/my_csv_file.csv'
os.system(DelLocalFile)
os.system(DelS3File)
 

datetime_object_2 = datetime.datetime.now()
print ("End TimeStamp")
print ("-------------")
print (datetime_object_2)
print ("")
 


