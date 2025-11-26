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

rds = session.client('rds')

# ----------------------------------------------------
# COMPROBAR SI LA INSTANCIA YA EXISTE
# ----------------------------------------------------

def instancia_existe(db_id):
    try:
        rds.describe_db_instances(DBInstanceIdentifier=db_id)
        return True
    except ClientError as e:
        if "DBInstanceNotFound" in str(e):
            return False
        else:
            raise  # Otro error real

# ----------------------------------------------------
# CREAR LA INSTANCIA SOLO SI NO EXISTE
# ----------------------------------------------------

if instancia_existe(DB_INSTANCE_ID):
    print(f"✔ La instancia {DB_INSTANCE_ID} ya existe. No se creará de nuevo.")
else:
    print(f"⏳ Creando instancia {DB_INSTANCE_ID}...")
    
    rds.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_ID,
        AllocatedStorage=20,
        DBInstanceClass="db.t4g.micro",
        Engine="mariadb",
        MasterUsername=os.getenv("DB_USER"),
        MasterUserPassword=os.getenv("DB_PASSWORD"),
        PubliclyAccessible=True
    )

    # Esperar a que esté disponible
    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=DB_INSTANCE_ID)

    print(f"✔ Instancia {DB_INSTANCE_ID} creada correctamente.")

# ----------------------------------------------------
# OBTENER ENDPOINT DE LA INSTANCIA
# ----------------------------------------------------

info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
endpoint = info['DBInstances'][0]['Endpoint']['Address']

print("Endpoint:", endpoint)

# ----------------------------------------------------
# CONECTARSE A MYSQL EN EL RDS Y CREAR TABLAS
# ----------------------------------------------------

SQL_TABLAS = """
CREATE TABLE IF NOT EXISTS Familias (
    id_familia INT AUTO_INCREMENT PRIMARY KEY,
    nombre_tutor VARCHAR(100),
    telefono_contacto VARCHAR(50),
    correo_electronico VARCHAR(100),
    direccion VARCHAR(200),
    situacion_laboral VARCHAR(100),
    nivel_educativo_tutor VARCHAR(100),
    ingresos_aprox DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS Profesorado (
    id_profesor INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100),
    apellidos VARCHAR(100),
    especialidad VARCHAR(100),
    correo_institucional VARCHAR(100),
    telefono_contacto VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Cursos (
    id_curso INT AUTO_INCREMENT PRIMARY KEY,
    nombre_curso VARCHAR(100),
    nivel_educativo VARCHAR(100),
    tutor INT,
    anio_escolar VARCHAR(20),
    FOREIGN KEY (tutor) REFERENCES Profesorado(id_profesor)
);

CREATE TABLE IF NOT EXISTS Alumnos (
    id_alumno INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100),
    apellidos VARCHAR(100),
    fecha_nacimiento DATE,
    genero VARCHAR(20),
    direccion VARCHAR(200),
    id_familia INT,
    id_curso INT,
    estado_escolar VARCHAR(50),
    FOREIGN KEY (id_familia) REFERENCES Familias(id_familia),
    FOREIGN KEY (id_curso) REFERENCES Cursos(id_curso)
);

CREATE TABLE IF NOT EXISTS Asignaturas (
    id_asignatura INT AUTO_INCREMENT PRIMARY KEY,
    nombre_asignatura VARCHAR(100),
    id_profesor INT,
    id_curso INT,
    horas_semanales INT,
    FOREIGN KEY (id_profesor) REFERENCES Profesorado(id_profesor),
    FOREIGN KEY (id_curso) REFERENCES Cursos(id_curso)
);

CREATE TABLE IF NOT EXISTS Calificaciones (
    id_calificacion INT AUTO_INCREMENT PRIMARY KEY,
    id_alumno INT,
    id_asignatura INT,
    trimestre INT,
    nota DECIMAL(5,2),
    observacion_docente TEXT,
    FOREIGN KEY (id_alumno) REFERENCES Alumnos(id_alumno),
    FOREIGN KEY (id_asignatura) REFERENCES Asignaturas(id_asignatura)
);

CREATE TABLE IF NOT EXISTS Asistencia (
    id_asistencia INT AUTO_INCREMENT PRIMARY KEY,
    id_alumno INT,
    fecha DATE,
    estado VARCHAR(20),
    observacion TEXT,
    FOREIGN KEY (id_alumno) REFERENCES Alumnos(id_alumno)
);

CREATE TABLE IF NOT EXISTS Incidencias (
    id_incidencia INT AUTO_INCREMENT PRIMARY KEY,
    id_alumno INT,
    tipo_incidencia VARCHAR(100),
    descripcion TEXT,
    fecha DATE,
    id_profesor INT,
    FOREIGN KEY (id_alumno) REFERENCES Alumnos(id_alumno),
    FOREIGN KEY (id_profesor) REFERENCES Profesorado(id_profesor)
);

CREATE TABLE IF NOT EXISTS Zonas (
    id_zona INT AUTO_INCREMENT PRIMARY KEY,
    nombre_zona VARCHAR(100),
    nivel_renta_promedio DECIMAL(10,2),
    tasa_desempleo DECIMAL(5,2),
    numero_entidades_apoyo INT
);

CREATE TABLE IF NOT EXISTS Servicios_Sociales (
    id_servicio INT AUTO_INCREMENT PRIMARY KEY,
    nombre_entidad VARCHAR(100),
    tipo_servicio VARCHAR(100),
    contacto VARCHAR(100),
    id_zona INT,
    FOREIGN KEY (id_zona) REFERENCES Zonas(id_zona)
);

CREATE TABLE IF NOT EXISTS Intervenciones (
    id_intervencion INT AUTO_INCREMENT PRIMARY KEY,
    id_alumno INT,
    id_servicio INT,
    tipo_intervencion VARCHAR(100),
    fecha_inicio DATE,
    fecha_fin DATE,
    resultado VARCHAR(100),
    FOREIGN KEY (id_alumno) REFERENCES Alumnos(id_alumno),
    FOREIGN KEY (id_servicio) REFERENCES Servicios_Sociales(id_servicio)
);
"""

def crear_mysql_mariadb(host, user, password, db_name):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    cursor.execute(f"USE {db_name};")

    for stmt in SQL_TABLAS.split(";"):
        if stmt.strip():
            cursor.execute(stmt)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✔ Base de datos '{db_name}' creada en {host}")

# Ejecutar creación de la DB en el RDS
crear_mysql_mariadb(endpoint, os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME"))

print("✨ ¡Proceso completado sin recreaciones innecesarias!")
