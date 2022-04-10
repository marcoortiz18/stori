import psycopg2
from pymongo import MongoClient
import boto3
import json


class Connector:

    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def connect_to_mongo(self):
        client = MongoClient(self.host)
            #select database
        connection = client[self.database]
        #select the collection within the database
        
        return connection

    def connect_to_pg(self):
        connection = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database
        )
        
        return connection

    def connect_to_pg(self):
        
        db_opts = {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'database': self.database
        }

        connection = psycopg2.connect(**db_opts)
        
        return connection

    def get_secrets(project):
        client = boto3.client("secretsmanager")
        pg_keys = client.get_secret_value(
            SecretId =project
        )

        secret = json.loads(pg_keys['SecretString'])

        return secret