import psycopg2
import pandas as pd
from pymongo import MongoClient
import psycopg2

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
        
        db_opts = {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'database': self.database
        }

        connection = psycopg2.connect(**db_opts)
        
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