# =============================================================================
# db_connection.py - MÓDULO DE CONEXIÓN A SQL SERVER
# =============================================================================
# Este módulo centraliza toda la lógica de conexión a la base de datos.
# Otros archivos (main.py, spark_job.py) lo importan para conectarse.
# Usamos pyodbc que habla con SQL Server via el driver ODBC de Microsoft.
# =============================================================================

import pyodbc
import os
import time
import logging

# Configuración básica de logs para ver qué pasa durante la ejecución
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ─── LEER CONFIGURACIÓN DESDE VARIABLES DE ENTORNO ───────────────────────────
# Las variables de entorno las define docker-compose.yml.
# Si no están definidas, usamos valores por defecto para desarrollo local.
SQL_HOST = os.getenv("SQL_SERVER_HOST", "localhost")
SQL_PORT = os.getenv("SQL_SERVER_PORT", "1433")
SQL_USER = os.getenv("SQL_SERVER_USER", "SA")
SQL_PASS = os.getenv("SQL_SERVER_PASS", "FavisaDB2024!")
SQL_DB   = os.getenv("SQL_SERVER_DB",   "FAVISA_DB")


def get_connection_string(database: str = SQL_DB) -> str:
    """
    Construye el string de conexión ODBC para SQL Server.
    
    Driver: usamos ODBC Driver 18 (instalado en el Dockerfile).
    TrustServerCertificate=yes: necesario para SQL Server en Docker
    porque no tiene un certificado SSL válido firmado por una CA.
    """
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"DATABASE={database};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASS};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )


def get_connection(database: str = SQL_DB, timeout: int = 30) -> pyodbc.Connection:
    """
    Establece conexión a SQL Server con reintentos automáticos.
    
    SQL Server tarda ~30-60s en arrancar la primera vez en Docker.
    Si falla, esperamos 5 segundos y volvemos a intentar.
    
    Args:
        database: nombre de la base de datos (default: FAVISA_DB)
        timeout:  segundos máximos de espera total
    
    Returns:
        pyodbc.Connection listo para usar
    """
    conn_str = get_connection_string(database)
    inicio = time.time()
    intento = 0

    while True:
        intento += 1
        try:
            log.info(f"Intento {intento}: conectando a SQL Server ({SQL_HOST}:{SQL_PORT}/{database})...")
            conn = pyodbc.connect(conn_str, timeout=10)
            log.info(f"✅ Conexión establecida con {database}")
            return conn
        except pyodbc.Error as e:
            elapsed = time.time() - inicio
            if elapsed >= timeout:
                log.error(f"❌ Timeout: no se pudo conectar en {timeout}s")
                raise
            log.warning(f"   No disponible aún (error: {str(e)[:80]}). Reintentando en 5s...")
            time.sleep(5)


def test_connection() -> bool:
    """
    Prueba conexión a master (FAVISA_DB aún no existe en este punto).
    """
    try:
        # Conectar a 'master', no a FAVISA_DB que todavía no existe
        conn = get_connection(database="master", timeout=120)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        log.info(f"SQL Server versión: {version.split(chr(10))[0]}")
        conn.close()
        return True
    except Exception as e:
        log.error(f"Error de conexión: {e}")
        return False


def execute_sql_file(filepath: str) -> bool:
    """
    Lee y ejecuta un archivo .sql en SQL Server.
    
    Divide el script en bloques separados por GO (separador de lotes
    en T-SQL que no es SQL estándar, así que hay que manejarlo manualmente).
    
    Args:
        filepath: ruta al archivo .sql
    
    Returns:
        True si se ejecutó sin errores, False si hubo algún problema
    """
    log.info(f"Ejecutando script SQL: {filepath}")

    try:
        # Leer el archivo con encoding que soporta caracteres latinos
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            sql_content = f.read()

        # Conectar al master primero (el script crea FAVISA_DB)
        conn = get_connection(database="master", timeout=120)
        conn.autocommit = True  # Necesario para CREATE DATABASE y ALTER DATABASE
        cursor = conn.cursor()

        # Dividir por GO (ignorando mayúsculas/minúsculas y espacios)
        import re
        # Divide en bloques donde GO aparece solo en su línea
        batches = re.split(r'^\s*GO\s*$', sql_content, flags=re.MULTILINE | re.IGNORECASE)

        exitosos = 0
        errores = 0
        for i, batch in enumerate(batches):
            batch = batch.strip()
            if not batch:  # saltar bloques vacíos
                continue
            try:
                cursor.execute(batch)
                exitosos += 1
            except pyodbc.Error as e:
                # Algunos errores son esperados (ej: ya existe la tabla)
                # Los logueamos pero continuamos
                error_msg = str(e)
                if "already exists" in error_msg or "ya existe" in error_msg:
                    log.debug(f"Bloque {i+1}: objeto ya existía (ignorado)")
                else:
                    log.warning(f"Bloque {i+1} error: {error_msg[:100]}")
                    errores += 1

        log.info(f"Script ejecutado: {exitosos} bloques OK, {errores} errores")
        conn.close()
        return errores == 0

    except Exception as e:
        log.error(f"Error ejecutando script SQL: {e}")
        return False


def check_table_has_data(table_name: str, min_rows: int = 1) -> bool:
    """
    Verifica si una tabla tiene datos.
    Usado para evitar cargar el CSV dos veces.
    
    Args:
        table_name: nombre de la tabla a verificar
        min_rows:   mínimo de filas para considerar que hay datos
    
    Returns:
        True si la tabla tiene al menos min_rows filas
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        log.info(f"Tabla {table_name}: {count:,} registros")
        return count >= min_rows
    except Exception as e:
        log.warning(f"No se pudo verificar tabla {table_name}: {e}")
        return False


def get_jdbc_url() -> str:
    """
    Devuelve la URL JDBC que necesita Spark para conectarse a SQL Server.
    Spark usa JDBC (Java Database Connectivity), diferente al ODBC de Python.
    Formato: jdbc:sqlserver://host:port;databaseName=DB;...
    """
    return (
        f"jdbc:sqlserver://{SQL_HOST}:{SQL_PORT};"
        f"databaseName={SQL_DB};"
        f"user={SQL_USER};"
        f"password={SQL_PASS};"
        f"trustServerCertificate=true;"
        f"encrypt=false"
    )


def get_jdbc_properties() -> dict:
    """
    Propiedades JDBC complementarias para la conexión de Spark.
    Se pasan como diccionario al método spark.read.jdbc().
    """
    return {
        "user":     SQL_USER,
        "password": SQL_PASS,
        "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "trustServerCertificate": "true",
        "encrypt": "false"
    }
