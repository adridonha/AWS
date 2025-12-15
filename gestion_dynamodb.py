import boto3
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv
from faker import Faker
import os

# Cargamos variables de entorno y Faker
load_dotenv()
fake = Faker("es_ES")

# Conexión con DynamoDB
session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

dynamodb = session.resource("dynamodb")
client = session.client("dynamodb")

print("\n--- CREACIÓN DE TABLAS EN DYNAMODB ---")

tables = client.list_tables()["TableNames"]

# Tabla sin índices
if "Alumnos" not in tables:
    dynamodb.create_table(
        TableName="Alumnos",
        KeySchema=[{"AttributeName": "id_alumno", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id_alumno", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST"
    )
    client.get_waiter("table_exists").wait(TableName="Alumnos")

# Tabla con índice local (LSI)
if "Cursos" not in tables:
    dynamodb.create_table(
        TableName="Cursos",
        KeySchema=[{"AttributeName": "id_curso", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id_curso", "AttributeType": "N"},
            {"AttributeName": "nivel", "AttributeType": "S"}
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "NivelIndex",
            "KeySchema": [
                {"AttributeName": "id_curso", "KeyType": "HASH"},
                {"AttributeName": "nivel", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        BillingMode="PAY_PER_REQUEST"
    )
    client.get_waiter("table_exists").wait(TableName="Cursos")

# Tabla con índice global (GSI)
if "Profesores" not in tables:
    dynamodb.create_table(
        TableName="Profesores",
        KeySchema=[{"AttributeName": "id_profesor", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id_profesor", "AttributeType": "N"},
            {"AttributeName": "especialidad", "AttributeType": "S"}
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "EspecialidadIndex",
            "KeySchema": [
                {"AttributeName": "especialidad", "KeyType": "HASH"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        BillingMode="PAY_PER_REQUEST"
    )
    client.get_waiter("table_exists").wait(TableName="Profesores")

print("Tablas creadas o ya existentes")

# Inserción de datos con Faker
print("\n--- INSERTANDO DATOS ---")

alumnos = dynamodb.Table("Alumnos")
cursos = dynamodb.Table("Cursos")
profesores = dynamodb.Table("Profesores")

for i in range(1, 4):
    alumnos.put_item(Item={
        "id_alumno": i,
        "nombre": fake.first_name()
    })

    cursos.put_item(Item={
        "id_curso": i,
        "nombre": fake.word(),
        "nivel": fake.random_element(["Primaria", "Secundaria"])
    })

    profesores.put_item(Item={
        "id_profesor": i,
        "nombre": fake.first_name(),
        "especialidad": fake.random_element(
            ["Matemáticas", "Lengua", "Ciencias"]
        )
    })

print("Datos insertados correctamente")

# Operaciones básicas
print("\n--- Obtener un registro ---")
print(alumnos.get_item(Key={"id_alumno": 1})["Item"])

print("\n--- Actualizar un registro ---")
alumnos.update_item(
    Key={"id_alumno": 1},
    UpdateExpression="SET nombre = :n",
    ExpressionAttributeValues={":n": fake.first_name()}
)

print("\n--- Eliminar un registro ---")
alumnos.delete_item(Key={"id_alumno": 2})

print("\n--- Obtener todos los registros ---")
print(alumnos.scan()["Items"])

print("\n--- Scan con filtro ---")
print(cursos.scan(
    FilterExpression=Attr("nivel").eq("Primaria")
)["Items"])

print("\n--- Query usando índice global ---")
print(profesores.query(
    IndexName="EspecialidadIndex",
    KeyConditionExpression=Key("especialidad").eq("Ciencias")
)["Items"])

print("\n--- Eliminación condicional ---")
for item in profesores.query(
    IndexName="EspecialidadIndex",
    KeyConditionExpression=Key("especialidad").eq("Lengua")
)["Items"]:
    profesores.delete_item(Key={"id_profesor": item["id_profesor"]})

print("\n--- Consulta PartiQL ---")
r = client.execute_statement(Statement='SELECT * FROM "Alumnos"')
print(r["Items"])
