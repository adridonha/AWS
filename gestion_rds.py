import mysql.connector
from dotenv import load_dotenv
from faker import Faker
import os

# Cargamos entorno y Faker
load_dotenv()
fake = Faker("es_ES")

# Conexión con RDS
cnx = mysql.connector.connect(
    host=os.getenv("RDS_ENDPOINT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = cnx.cursor()

# Crear base de datos
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('DB_NAME')}")
cnx.database = os.getenv("DB_NAME")

# Crear tabla simple
cursor.execute("""
CREATE TABLE IF NOT EXISTS Alumnos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100),
    email VARCHAR(100)
)
""")

cnx.commit()

# Insertar datos con Faker
print("\n--- INSERTANDO DATOS EN RDS ---")

for _ in range(3):
    cursor.execute(
        "INSERT INTO Alumnos (nombre, email) VALUES (%s, %s)",
        (fake.name(), fake.email())
    )

cnx.commit()

# Consultas
print("\n--- CONSULTA 1 ---")
cursor.execute("SELECT * FROM Alumnos")
print(cursor.fetchall())

print("\n--- CONSULTA 2 ---")
cursor.execute("SELECT nombre FROM Alumnos")
print(cursor.fetchall())

print("\n--- CONSULTA 3 ---")
cursor.execute("SELECT * FROM Alumnos WHERE nombre LIKE 'A%'")
print(cursor.fetchall())

cursor.close()
cnx.close()
