import psycopg2

from Connector import Connector

class Redshift:

    def copy_to_redshift(file_name):
        from_path = "s3://stori-bucket/{}".format(file_name)

        # Get all the secrets keys
        secret = Connector.get_secrets("dev/stori/redshift")
        dbname = secret['db_name']
        host = secret['host']
        user = secret['username']
        password = secret['password']
        iam_role = secret['iam_role']
        schema = secret['schema']
        redshift_port = secret['port']
        tablename = secret['table_name']
        if "mongo_" in file_name:
            tablename = secret['table_name_mongo']

        connection = psycopg2.connect(  dbname = dbname,
                                        host = host,
                                        port = redshift_port,
                                        user = user,
                                        password = password)
                                        
        curs = connection.cursor()
        query = "COPY {}.{} FROM '{}' IAM_ROLE '{}' FORMAT AS PARQUET;".format(schema, tablename, from_path, iam_role)
        print("query {}".format(query))
        
        curs.execute(query)
        connection.commit()
        curs.close()
        connection.close()
        print('data saved....')
