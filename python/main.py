# =============================================================================
# main.py - ORQUESTADOR PRINCIPAL DEL SISTEMA FAVISA
# =============================================================================
# Este archivo es el punto de entrada del proyecto.
# Ejecuta en orden:
#   1. Verifica conexión a SQL Server
#   2. Ejecuta el script SQL (crea BD + tablas)
#   3. Carga el CSV a la tabla staging
#   4. Normaliza y migra datos a tablas definitivas
#   5. Lanza el análisis con Spark
#   6. Guarda resultados de Spark en SQL Server
#   7. Muestra resumen en consola
#
# Ejecución:  python main.py
# =============================================================================

import os
import sys
import time
import logging
import csv
import re

# ─── CONFIGURACIÓN DE LOGGING ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("FAVISA_MAIN")

# ─── IMPORTAR MÓDULO DE CONEXIÓN PROPIO ───────────────────────────────────────
# db_connection.py está en la misma carpeta /app/python/
sys.path.insert(0, '/app/python')
from db_connection import (
    test_connection, execute_sql_file, check_table_has_data,
    get_connection
)

# ─── RUTAS DE ARCHIVOS (desde variables de entorno o valores por defecto) ────
CSV_PATH = os.getenv("CSV_PATH", "/app/data/DataOZ_ChimboteNuevoCampo_03.csv")
SQL_PATH = "/app/sql/init.sql"

# ─── BATCH SIZE: cuántas filas insertar por lote en SQL Server ───────────────
# Insertar de a miles evita timeouts y reduce memoria usada.
BATCH_SIZE = 500


# =============================================================================
# FUNCIONES DE LIMPIEZA/NORMALIZACIÓN DE DATOS
# =============================================================================

def limpiar_estado(valor: str) -> str:
    """
    Normaliza valores del campo ddp_estado.
    El CSV real contiene valores sucios como '2ACTIVO'.
    Los datos se mapean a los valores válidos del catálogo ESTADO_TRIBUTARIO.
    """
    if not valor:
        return 'ND'
    v = str(valor).strip().upper()
    # Limpiar prefijos numéricos corruptos
    if v == '2ACTIVO':
        return 'ACTIVO'
    if v in ('ACTIVO', '10', '11', '3', '2', '1', '12', 'INACTIVO'):
        return v
    return 'ND'


def limpiar_condicion(valor: str) -> str:
    """
    Normaliza valores del campo ddp_flag22.
    El CSV real contiene '2HABIDO' y otros valores corruptos.
    """
    if not valor:
        return 'ND'
    v = str(valor).strip().upper()
    if v == '2HABIDO':
        return 'HABIDO'
    # Valores numéricos del 1 al 12 son válidos, más allá no
    if v in ('HABIDO', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'):
        return v
    return 'ND'


def limpiar_tipo_empresa(valor: str) -> str:
    """
    Normaliza tipos de empresa.
    El CSV tiene valores como 'A7', 'B9', 'AA', 'AB', etc.
    Solo el primer carácter suele ser válido; el resto es corrupción.
    Los tipos válidos en el catálogo son: A, B, C, D, E, CE, -
    """
    if not valor:
        return '-'
    v = str(valor).strip().upper()
    # Tipos exactos válidos del catálogo
    if v in ('A', 'B', 'C', 'D', 'E', 'CE', '-'):
        return v
    # Si tiene exactamente 2 chars como 'CE' ya está cubierto arriba
    # Para otros 2-chars corruptos: tomar primer char si es letra válida
    if len(v) >= 1 and v[0] in ('A', 'B', 'C', 'D', 'E'):
        return v[0]
    return '-'


def limpiar_sexo(valor: str) -> str:
    """Normaliza sexo al constraint CHECK de la tabla."""
    if not valor:
        return 'ND'
    v = str(valor).strip().upper()
    if v in ('HOMBRE', 'MUJER'):
        return v
    return 'ND'


def limpiar_tamano(valor: str) -> str:
    """Normaliza tamaño al catálogo (C, M, G, B)."""
    if not valor:
        return 'C'  # default: pequeña empresa
    v = str(valor).strip().upper()
    if v in ('C', 'M', 'G', 'B'):
        return v
    return 'C'


# =============================================================================
# PASO 3: CARGAR CSV A LA BASE DE DATOS
# =============================================================================

def cargar_csv_a_staging(conn) -> int:
    """
    Lee el archivo CSV y lo inserta en TMP_DataOZ_Staging.
    
    Usa INSERT en lotes (BATCH_SIZE filas) para ser eficiente.
    Limpia la tabla antes de insertar para evitar duplicados si se re-ejecuta.
    
    Returns:
        Número de filas insertadas exitosamente
    """
    log.info(f"📂 Cargando CSV: {CSV_PATH}")
    
    cursor = conn.cursor()
    
    # Limpiar staging por si tiene datos de una ejecución anterior
    cursor.execute("DELETE FROM TMP_DataOZ_Staging")
    conn.commit()
    
    sql_insert = """
        INSERT INTO TMP_DataOZ_Staging 
        (ddp_numruc, ddp_ciiu, ddp_tpoemp, ddp_tamano, ddp_ubigeo, 
         ddp_estado, ddp_flag22, dds_sexo, dds_edad, deuda, condicion)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    filas_ok = 0
    filas_error = 0
    batch = []
    
    # Leer CSV con encoding latin-1 (el archivo original tiene este encoding)
    with open(CSV_PATH, 'r', encoding='latin-1') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader):
            try:
                # Convertir y validar cada campo
                ruc = int(row['ddp_numruc']) if row['ddp_numruc'].strip() else None
                edad = int(row['dds_edad']) if row['dds_edad'].strip() else None
                deuda = float(row['deuda']) if row['deuda'].strip() else 0.0
                condicion = int(row['condicion']) if row['condicion'].strip() else 0
                
                # Validar RUC: ignorar filas sin RUC
                if ruc is None:
                    continue
                
                batch.append((
                    ruc,
                    row['ddp_ciiu'].strip()  or None,
                    row['ddp_tpoemp'].strip() or None,
                    row['ddp_tamano'].strip() or None,
                    row['ddp_ubigeo'].strip() or None,
                    row['ddp_estado'].strip() or None,
                    row['ddp_flag22'].strip() or None,
                    row['dds_sexo'].strip()   or None,
                    edad,
                    deuda,
                    condicion
                ))
                
                # Cuando el lote está lleno, insertar y limpiar
                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(sql_insert, batch)
                    conn.commit()
                    filas_ok += len(batch)
                    batch = []
                    
                    # Mostrar progreso cada 10,000 filas
                    if filas_ok % 10000 == 0:
                        log.info(f"   ...{filas_ok:,} filas insertadas en staging")
                        
            except (ValueError, KeyError) as e:
                filas_error += 1
                if filas_error <= 5:  # solo mostrar los primeros 5 errores
                    log.debug(f"Fila {i+2} ignorada: {e}")
    
    # Insertar el último lote (que puede ser < BATCH_SIZE)
    if batch:
        cursor.executemany(sql_insert, batch)
        conn.commit()
        filas_ok += len(batch)
    
    log.info(f"✅ CSV cargado: {filas_ok:,} filas OK, {filas_error} ignoradas")
    return filas_ok


# =============================================================================
# PASO 4: POBLAR CATÁLOGOS DESDE STAGING
# =============================================================================

def poblar_catalogos(conn):
    """
    Lee los valores únicos del staging y los inserta en los catálogos
    que aún no los tengan. Esto incluye los CIIU y las UBICACIONES,
    que son demasiados para hardcodear en el SQL.
    """
    log.info("📚 Poblando catálogos desde datos del CSV...")
    cursor = conn.cursor()
    
    # ── Actividades económicas (CIIU) ────────────────────────────────────────
    # El CSV tiene miles de códigos CIIU únicos; es inviable listarlos en el SQL.
    cursor.execute("""
        INSERT INTO ACTIVIDAD_ECONOMICA (id_ciiu, descripcion, division)
        SELECT DISTINCT
            ddp_ciiu,
            'Actividad económica código ' + ddp_ciiu,
            LEFT(ddp_ciiu, 2)
        FROM TMP_DataOZ_Staging
        WHERE ddp_ciiu IS NOT NULL
          AND ddp_ciiu NOT IN (SELECT id_ciiu FROM ACTIVIDAD_ECONOMICA)
    """)
    conn.commit()
    log.info(f"   CIIUs insertados: {cursor.rowcount:,}")
    
    # ── Ubicaciones geográficas ───────────────────────────────────────────────
    # El CSV tiene ubigeos con nombres de distritos y códigos numéricos.
    cursor.execute("""
        INSERT INTO UBICACION_GEOGRAFICA (id_ubicacion, nombre_distrito, provincia, departamento)
        SELECT DISTINCT
            ddp_ubigeo,
            ddp_ubigeo,
            'Santa',
            'Áncash'
        FROM TMP_DataOZ_Staging
        WHERE ddp_ubigeo IS NOT NULL
          AND ddp_ubigeo NOT IN (SELECT id_ubicacion FROM UBICACION_GEOGRAFICA)
    """)
    conn.commit()
    log.info(f"   Ubicaciones insertadas: {cursor.rowcount:,}")
    
    # ── Tipos de empresa (los "corruptos" van a '-' que ya existe) ───────────
    # Solo insertamos los que no existen y son válidos
    cursor.execute("""
        SELECT DISTINCT ddp_tpoemp FROM TMP_DataOZ_Staging WHERE ddp_tpoemp IS NOT NULL
    """)
    tipos_csv = [r[0] for r in cursor.fetchall()]
    
    nuevos_tipos = 0
    for tipo in tipos_csv:
        tipo_limpio = limpiar_tipo_empresa(tipo)
        # El tipo limpio ya debe existir en el catálogo; sino insertamos el original
        cursor.execute("SELECT COUNT(*) FROM TIPO_EMPRESA WHERE id_tipo_empresa = ?", tipo_limpio)
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                "INSERT INTO TIPO_EMPRESA (id_tipo_empresa, descripcion, abreviatura) VALUES (?,?,?)",
                (tipo_limpio, f'Tipo {tipo_limpio}', tipo_limpio)
            )
            nuevos_tipos += 1
    conn.commit()
    if nuevos_tipos:
        log.info(f"   Tipos empresa adicionales: {nuevos_tipos}")


# =============================================================================
# PASO 5: MIGRAR DATOS DE STAGING A TABLA PRINCIPAL
# =============================================================================

def migrar_staging_a_contribuyente(conn) -> int:
    """
    Lee TMP_DataOZ_Staging, aplica limpieza de datos y migra a CONTRIBUYENTE.
    
    La limpieza consiste en:
    - Normalizar valores sucios (2ACTIVO → ACTIVO, A7 → A, etc.)
    - Filtrar RUCs inválidos
    - Resolver FKs con los catálogos existentes
    
    Returns:
        Número de registros migrados
    """
    log.info("🚀 Migrando datos de staging a CONTRIBUYENTE...")
    
    cursor = conn.cursor()
    
    # Limpiar la tabla principal para re-ejecuciones limpias
    cursor.execute("DELETE FROM CONTRIBUYENTE")
    conn.commit()
    
    # Leer todos los registros del staging
    cursor.execute("""
        SELECT ddp_numruc, ddp_ciiu, ddp_tpoemp, ddp_tamano, ddp_ubigeo,
               ddp_estado, ddp_flag22, dds_sexo, dds_edad, deuda
        FROM TMP_DataOZ_Staging
        WHERE ddp_numruc >= 10000 -- filtrar RUCs claramente inválidos
          AND ddp_ciiu IS NOT NULL
          AND ddp_tpoemp IS NOT NULL
          AND ddp_tamano IS NOT NULL
          AND ddp_ubigeo IS NOT NULL
          AND ddp_estado IS NOT NULL
          AND ddp_flag22 IS NOT NULL
    """)
    rows = cursor.fetchall()
    log.info(f"   Filas del staging a procesar: {len(rows):,}")
    
    sql_insert = """
        INSERT INTO CONTRIBUYENTE
        (ruc, id_ciiu, id_tipo_empresa, id_tamano, id_ubicacion,
         id_estado, id_condicion, sexo, edad, deuda, flag22)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    batch = []
    insertados = 0
    descartados = 0
    ruc_set = set()  # para evitar RUCs duplicados en el lote
    
    for row in rows:
        ruc, ciiu, tpoemp, tamano, ubigeo, estado, flag22, sexo, edad, deuda = row
        
        # Aplicar limpieza a los campos que lo necesitan
        estado_limpio    = limpiar_estado(str(estado) if estado else '')
        condicion_limpia = limpiar_condicion(str(flag22) if flag22 else '')
        tipo_limpio      = limpiar_tipo_empresa(str(tpoemp) if tpoemp else '')
        tamano_limpio    = limpiar_tamano(str(tamano) if tamano else '')
        sexo_limpio      = limpiar_sexo(str(sexo) if sexo else '')
        
        # Normalizar deuda
        deuda_val = float(deuda) if deuda is not None else 0.0
        if deuda_val < 0:
            deuda_val = 0.0
        
        # Evitar duplicados de RUC
        if ruc in ruc_set:
            descartados += 1
            continue
        ruc_set.add(ruc)
        
        batch.append((
            ruc, ciiu, tipo_limpio, tamano_limpio, ubigeo,
            estado_limpio, condicion_limpia,
            sexo_limpio, edad, deuda_val,
            flag22  # guardamos el valor original en flag22 para referencia
        ))
        
        if len(batch) >= BATCH_SIZE:
            try:
                cursor.executemany(sql_insert, batch)
                conn.commit()
                insertados += len(batch)
            except Exception as e:
                # Si falla el lote, insertar uno a uno para identificar el problemático
                conn.rollback()
                for single_row in batch:
                    try:
                        cursor.execute(sql_insert, single_row)
                        conn.commit()
                        insertados += 1
                    except Exception:
                        descartados += 1
                        conn.rollback()
            batch = []
            
            if insertados % 10000 == 0:
                log.info(f"   ...{insertados:,} registros migrados")
    
    # Último lote
    if batch:
        try:
            cursor.executemany(sql_insert, batch)
            conn.commit()
            insertados += len(batch)
        except Exception:
            conn.rollback()
            for single_row in batch:
                try:
                    cursor.execute(sql_insert, single_row)
                    conn.commit()
                    insertados += 1
                except Exception:
                    descartados += 1
                    conn.rollback()
    
    log.info(f"✅ Migración completa: {insertados:,} registros OK, {descartados:,} descartados")
    
    # Agregar el registro especial del script original
    try:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM CONTRIBUYENTE WHERE ruc = 611077)
            INSERT INTO CONTRIBUYENTE (ruc, id_ciiu, id_tipo_empresa, id_tamano, id_ubicacion,
                                       id_estado, id_condicion, sexo, edad, deuda, flag22)
            VALUES (611077, '75113', 'A', 'C', 'CHIMBOTE', 'ACTIVO', 'HABIDO', 'ND', 0, 0.00, 'HABIDO')
        """)
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo insertar registro especial: {e}")
    
    return insertados


# =============================================================================
# PASO 6: EJECUTAR ANÁLISIS CON SPARK
# =============================================================================

def ejecutar_spark():
    """
    Invoca spark_job.py en el mismo proceso Python.
    Spark corre localmente en modo 'local[*]' dentro del contenedor,
    pero también puede conectarse al cluster Spark (spark-master).
    """
    log.info("⚡ Iniciando análisis con Apache Spark...")
    
    try:
        # Importar y ejecutar el job de Spark
        import spark_job
        spark_job.run()
        log.info("✅ Análisis Spark completado")
        return True
    except Exception as e:
        log.error(f"❌ Error en Spark: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# PASO 7: MOSTRAR RESUMEN FINAL
# =============================================================================

def mostrar_resumen(conn):
    """
    Consulta la BD y muestra un resumen ejecutivo en consola.
    Datos que un gerente de FAVISA querría ver de un vistazo.
    """
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("  RESUMEN EJECUTIVO - FAVISA / CONTRIBUYENTES CHIMBOTE")
    print("="*60)
    
    # Total de registros
    cursor.execute("SELECT COUNT(*) FROM CONTRIBUYENTE")
    total = cursor.fetchone()[0]
    print(f"\n📊 Total contribuyentes registrados: {total:,}")
    
    # Por estado tributario
    cursor.execute("""
        SELECT et.descripcion, COUNT(*) as total
        FROM CONTRIBUYENTE c
        JOIN ESTADO_TRIBUTARIO et ON c.id_estado = et.id_estado
        GROUP BY et.descripcion
        ORDER BY total DESC
    """)
    print("\n📋 Por estado tributario:")
    for row in cursor.fetchall():
        print(f"   {row[0]:<35} {row[1]:>8,}")
    
    # Deuda total y promedio
    cursor.execute("SELECT SUM(deuda), AVG(deuda), MAX(deuda), COUNT(*) FROM CONTRIBUYENTE WHERE deuda > 0")
    r = cursor.fetchone()
    if r[0]:
        print(f"\n💰 Deuda total:     S/ {r[0]:>15,.2f}")
        print(f"   Deuda promedio:  S/ {r[1]:>15,.2f}")
        print(f"   Deuda máxima:    S/ {r[2]:>15,.2f}")
        print(f"   Con deuda:       {r[3]:,} contribuyentes")
    
    # Top 5 distritos
    cursor.execute("""
        SELECT TOP 5 ug.nombre_distrito, COUNT(*) as total
        FROM CONTRIBUYENTE c
        JOIN UBICACION_GEOGRAFICA ug ON c.id_ubicacion = ug.id_ubicacion
        GROUP BY ug.nombre_distrito
        ORDER BY total DESC
    """)
    print("\n📍 Top 5 distritos:")
    for row in cursor.fetchall():
        print(f"   {row[0]:<30} {row[1]:>8,}")
    
    # Resultados Spark si existen
    cursor.execute("SELECT COUNT(*) FROM RESULTADO_SPARK")
    spark_results = cursor.fetchone()[0]
    if spark_results > 0:
        print(f"\n⚡ Resultados de análisis Spark guardados: {spark_results}")
    
    # Tabla de verificación final (igual al script SQL original)
    print("\n📦 Registros por tabla:")
    tablas = ['CONTRIBUYENTE', 'ACTIVIDAD_ECONOMICA', 'TIPO_EMPRESA',
              'TAMANO_EMPRESA', 'UBICACION_GEOGRAFICA', 'ESTADO_TRIBUTARIO', 'CONDICION_DOMICILIO']
    for tabla in tablas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        n = cursor.fetchone()[0]
        print(f"   {tabla:<30} {n:>8,}")
    
    print("\n" + "="*60)
    print("  Dashboard disponible en: http://localhost:8501")
    print("  Spark UI disponible en:  http://localhost:8080")
    print("="*60 + "\n")


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """
    Punto de entrada. Orquesta todos los pasos en orden.
    """
    log.info("="*55)
    log.info("  SISTEMA FAVISA - INICIANDO ORQUESTADOR")
    log.info("="*55)
    
    # ── PASO 1: Verificar conexión ───────────────────────────────────────────
    log.info("\n[PASO 1] Verificando conexión a SQL Server...")
    if not test_connection():
        log.error("No se pudo conectar a SQL Server. Abortando.")
        sys.exit(1)
    
    # ── PASO 2: Ejecutar script SQL (crea BD y tablas) ───────────────────────
    log.info("\n[PASO 2] Ejecutando script SQL de inicialización...")
    if not execute_sql_file(SQL_PATH):
        log.warning("El script SQL tuvo algunos errores (puede ser normal en re-ejecuciones)")
    
    # ── PASO 3: Conectar a FAVISA_DB ─────────────────────────────────────────
    conn = get_connection()
    
    # ── PASO 3-4: Cargar CSV (solo si no hay datos) ──────────────────────────
    log.info("\n[PASO 3] Verificando si necesita cargar CSV...")
    if not check_table_has_data("CONTRIBUYENTE", min_rows=100):
        log.info("   La tabla está vacía. Cargando CSV...")
        
        filas_staging = cargar_csv_a_staging(conn)
        log.info(f"   Staging con {filas_staging:,} filas")
        
        log.info("\n[PASO 4] Poblando catálogos...")
        poblar_catalogos(conn)
        
        log.info("\n[PASO 5] Migrando datos a tabla CONTRIBUYENTE...")
        migrar_staging_a_contribuyente(conn)
    else:
        log.info("   CONTRIBUYENTE ya tiene datos. Saltando carga de CSV.")
    
    # ── PASO 6: Ejecutar Spark ───────────────────────────────────────────────
    log.info("\n[PASO 6] Ejecutando análisis con Apache Spark...")
    ejecutar_spark()
    
    # ── PASO 7: Mostrar resumen ──────────────────────────────────────────────
    log.info("\n[PASO 7] Generando resumen final...")
    mostrar_resumen(conn)
    
    conn.close()
    log.info("🎉 Orquestador finalizado exitosamente.")


if __name__ == "__main__":
    main()
