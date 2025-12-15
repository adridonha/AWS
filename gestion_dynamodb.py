import boto3
from boto3.dynamodb.conditions import Attr, Key
from dotenv import load_dotenv
from faker import Faker
import os
import time

load_dotenv()
fake = Faker("es_ES")

# -------------------------------
# Conexión a DynamoDB
# -------------------------------
session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

dynamodb = session.resource("dynamodb")
client = session.client("dynamodb")

# -------------------------------
# Creación de tablas
# -------------------------------
print("\n--- CREACIÓN DE TABLAS EN DYNAMODB ---")

# 1️⃣ Tabla sin índices
try:
    dynamodb.create_table(
        TableName="Alumnos",
        KeySchema=[{"AttributeName": "id_alumno", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id_alumno", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Tabla Alumnos creada")
except client.exceptions.ResourceInUseException:
    print("Tabla Alumnos ya existe")

# 2️⃣ Tabla con índice local (LSI)
try:
    dynamodb.create_table(
        TableName="Cursos",
        KeySchema=[
            {"AttributeName": "id_curso", "KeyType": "HASH"},  # Partition Key
            {"AttributeName": "semestre", "KeyType": "RANGE"}  # Sort Key (OBLIGATORIO para tener LSI)
        ],
        AttributeDefinitions=[
            {"AttributeName": "id_curso", "AttributeType": "N"},
            {"AttributeName": "semestre", "AttributeType": "S"},
            {"AttributeName": "nivel", "AttributeType": "S"}
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "NivelIndex",
            "KeySchema": [
                {"AttributeName": "id_curso", "KeyType": "HASH"}, # Debe coincidir con la tabla
                {"AttributeName": "nivel", "KeyType": "RANGE"}    # La clave alternativa
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Tabla Cursos creada con LSI")
except client.exceptions.ResourceInUseException:
    print("Tabla Cursos ya existe")

# 3️⃣ Tabla con índice global (GSI)
try:
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
    print("Tabla Profesores creada")
except client.exceptions.ResourceInUseException:
    print("Tabla Profesores ya existe")

time.sleep(5)

print("⏳ Esperando a que las tablas estén activas...")

# Crear el waiter
waiter = client.get_waiter('table_exists')

# Esperar por cada tabla crítica que acabas de crear
waiter.wait(TableName="Alumnos")
waiter.wait(TableName="Cursos")
waiter.wait(TableName="Profesores")

print("✅ Todas las tablas están listas. Continuando con la inserción.")

# -------------------------------
# Insertar registros con Faker
# -------------------------------
print("\n--- INSERTANDO REGISTROS ---")

alumnos = dynamodb.Table("Alumnos")
cursos = dynamodb.Table("Cursos")
profesores = dynamodb.Table("Profesores")

for i in range(1, 4):
    alumnos.put_item(Item={
        "id_alumno": i,
        "nombre": fake.first_name(),
        "curso": fake.random_element(["5A", "5B", "6A"])
    })

    cursos.put_item(Item={
        "id_curso": i,
        "semestre": "2024-Q1",  # <--- NUEVO: Necesario porque ahora es parte de la clave
        "nombre_curso": fake.random_element(["5A", "5B", "6A"]),
        "nivel": fake.random_element(["Primaria", "Secundaria"])
    })

    profesores.put_item(Item={
        "id_profesor": i,
        "nombre": fake.first_name(),
        "especialidad": fake.random_element(["Matemáticas", "Lengua", "Ciencias"])
    })

print("Registros insertados")

# -------------------------------
# Obtener un registro
# -------------------------------
print("\n--- OBTENER UN REGISTRO ---")
print(alumnos.get_item(Key={"id_alumno": 1})["Item"])
print(cursos.get_item(Key={
    "id_curso": 1, 
    "semestre": "2024-Q1"
})["Item"])
print(profesores.get_item(Key={"id_profesor": 1})["Item"])

# -------------------------------
# Actualizar un registro
# -------------------------------
print("\n--- ACTUALIZAR REGISTRO ---")

alumnos.update_item(
    Key={"id_alumno": 1},
    UpdateExpression="SET nombre = :n",
    ExpressionAttributeValues={":n": "Actualizado"}
)

print("Alumno actualizado")

# -------------------------------
# Eliminar un registro
# -------------------------------
print("\n--- ELIMINAR REGISTRO ---")

alumnos.delete_item(Key={"id_alumno": 3})
cursos.delete_item(Key={
    "id_curso": 3,
    "semestre": "2024-Q1"
})
profesores.delete_item(Key={"id_profesor": 3})

print("Registros eliminados")

# -------------------------------
# Obtener todos los registros
# -------------------------------
print("\n--- TODOS LOS REGISTROS ---")
print(alumnos.scan()["Items"])
print(cursos.scan()["Items"])
print(profesores.scan()["Items"])

# -------------------------------
# Scan con filtros
# -------------------------------
print("\n--- SCAN CON FILTROS ---")

print(alumnos.scan(
    FilterExpression=Attr("nombre").eq("Actualizado")
)["Items"])

print(cursos.scan(
    FilterExpression=Attr("nivel").eq("Primaria")
)["Items"])

# Usando el índice global
print(profesores.query(
    IndexName="EspecialidadIndex",
    KeyConditionExpression=Key("especialidad").eq("Ciencias")
)["Items"])

# -------------------------------
# Eliminación condicional
# -------------------------------
print("\n--- ELIMINACIÓN CONDICIONAL ---")

for item in alumnos.scan()["Items"]:
    if item["nombre"] == "Actualizado":
        alumnos.delete_item(Key={"id_alumno": item["id_alumno"]})
        print("Alumno eliminado condicionalmente")

# -------------------------------
# Varios filtros
# -------------------------------
print("\n--- VARIOS FILTROS ---")

print(cursos.scan(
    FilterExpression=Attr("nivel").eq("Primaria") & Attr("nombre_curso").contains("5")
)["Items"])

# -------------------------------
# PartiQL
# -------------------------------
print("\n--- CONSULTAS PARTIQL ---")

for tabla in ["Alumnos", "Cursos", "Profesores"]:
    response = client.execute_statement(
        Statement=f'SELECT * FROM "{tabla}"'
    )
    print(tabla, response["Items"])
