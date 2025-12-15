import json
import boto3
import mysql.connector
from dotenv import load_dotenv
import os
from decimal import Decimal

# Función sencilla para limpiar Decimals
def limpiar(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

load_dotenv()

# DynamoDB
session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

dynamodb = session.resource("dynamodb")
alumnos_dynamo = dynamodb.Table("Alumnos").scan()["Items"]

# RDS
cnx = mysql.connector.connect(
    host=os.getenv("RDS_ENDPOINT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)

cursor = cnx.cursor(dictionary=True)
cursor.execute("SELECT * FROM Alumnos")
alumnos_rds = cursor.fetchall()

cursor.close()
cnx.close()

# Unimos la información
data = {
    "dynamodb": alumnos_dynamo,
    "rds": alumnos_rds
}

with open("datos_unificados.json", "w", encoding="utf-8") as f:
    json.dump(data, f, default=limpiar, indent=4, ensure_ascii=False)

print("Archivo datos_unificados.json creado correctamente")