import boto3
from dotenv import load_dotenv
import os
import mysql.connector
from botocore.exceptions import ClientError

load_dotenv()

DB_INSTANCE_ID = os.getenv("DB_INSTANCE_ID")

session = boto3.session.Session(
   aws_access_key_id=os.getenv("ACCESS_KEY"),
   aws_secret_access_key=os.getenv("SECRET_KEY"),
   aws_session_token=os.getenv("SESSION_TOKEN"),
   region_name=os.getenv("REGION")
)

dynamodb = session.client('dynamodb')

tabla = dynamodb.create_table(
   TableName='ejemplo_tabla_libros',
   AttributeDefinitions=[
       {'AttributeName': 'autor', 'AttributeType': 'S'},   # Partition Key
       {'AttributeName': 'anyo_publicacion', 'AttributeType': 'S'}     # Sort Key
   ],
   KeySchema=[
       {'AttributeName': 'autor', 'KeyType': 'HASH'},  # Partition Key
       {'AttributeName': 'anyo_publicacion', 'KeyType': 'RANGE'}   # Sort Key
   ],
   ProvisionedThroughput={
       'ReadCapacityUnits': 5, #Numero de lecturas por segundo
       'WriteCapacityUnits': 5 #Numero de escrituras por segundo
   }
)
