# =============================================================================
# spark_job.py - ANÁLISIS DE DATOS CON APACHE SPARK
# =============================================================================
# Este módulo realiza análisis reales sobre los datos de contribuyentes
# de Chimbote usando PySpark, conectándose directamente a SQL Server via JDBC.
#
# Contexto del negocio (FAVISA / Grupo X-Y):
# FAVISA opera en: abarrotes/supermercado, galería CHIC (alquiler de stands),
# salón de eventos, y conserva de pescado. Los contribuyentes del CSV son
# el ecosistema comercial de Chimbote donde FAVISA compite o con quienes
# se relaciona (proveedores, competidores, clientes B2B).
#
# Los análisis muestran:
#   A. Salud fiscal del ecosistema (deuda, estados)
#   B. Distribución geográfica (distritos clave)
#   C. Estructura empresarial (tipos, tamaños)
#   D. Sectores económicos relevantes para FAVISA (CIIU)
#   E. Demografía de propietarios (sexo, edad)
# =============================================================================

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Importar módulo de conexión para guardar resultados
sys.path.insert(0, '/app/python')
from db_connection import get_jdbc_url, get_jdbc_properties, get_connection

log = logging.getLogger("FAVISA_SPARK")

# Ruta al JAR JDBC de SQL Server (descargado en el Dockerfile)
JDBC_JAR = "/opt/spark-jars/mssql-jdbc-12.4.2.jre11.jar"

# URL del master Spark (del docker-compose)
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "local[*]")


def crear_spark_session() -> SparkSession:
    """
    Crea la sesión de Spark configurada para conectarse a SQL Server.
    
    SPARK_MASTER puede ser:
    - 'local[*]'                 → corre dentro del contenedor Python (más simple)
    - 'spark://spark-master:7077' → usa el cluster Spark del docker-compose
    
    El JAR JDBC es esencial: sin él Spark no sabe hablar con SQL Server.
    """
    log.info(f"Iniciando Spark Session (master: {SPARK_MASTER})")
    
    spark = (SparkSession.builder
        .appName("FAVISA_Analisis_Contribuyentes")
        .master(SPARK_MASTER)
        # Agregar el JAR del driver JDBC de SQL Server al classpath
        .config("spark.jars", JDBC_JAR)
        # Configuración de memoria para manejar 50K registros sin problemas
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        # Logs menos verbosos (solo WARNING y ERROR)
        .config("spark.ui.showConsoleProgress", "false")
        # Número de particiones para shuffle (reducir overhead con datos medianos)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    
    # Reducir verbosidad de logs de Spark
    spark.sparkContext.setLogLevel("WARN")
    
    log.info(f"✅ Spark Session creada. Versión: {spark.version}")
    return spark


def leer_tabla_spark(spark: SparkSession, tabla: str):
    """
    Lee una tabla completa de SQL Server usando Spark JDBC.
    
    JDBC permite leer directamente desde SQL Server como si fuera un
    DataFrame de Spark. Spark distribuye la lectura entre sus workers.
    
    Args:
        spark: SparkSession activa
        tabla: nombre de la tabla o vista en SQL Server
    
    Returns:
        DataFrame de Spark con los datos de la tabla
    """
    url = get_jdbc_url()
    props = get_jdbc_properties()
    
    log.info(f"   Leyendo tabla '{tabla}' desde SQL Server via JDBC...")
    
    df = (spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", tabla)
        .option("driver", props["driver"])
        .option("user", props["user"])
        .option("password", props["password"])
        .option("trustServerCertificate", "true")
        .option("encrypt", "false")
        # numPartitions: cuántas conexiones paralelas abrir
        # fetchsize: cuántas filas traer por batch JDBC
        .option("numPartitions", "4")
        .option("fetchsize", "1000")
        .load()
    )
    
    count = df.count()
    log.info(f"   '{tabla}': {count:,} registros cargados en Spark")
    return df


def guardar_resultado_spark(nombre: str, data: list, conn):
    """
    Persiste los resultados del análisis Spark en la tabla RESULTADO_SPARK.
    Así los resultados quedan en la BD y el dashboard puede leerlos.
    
    Args:
        nombre: nombre del análisis (ej: 'Distribucion por Estado')
        data:   lista de tuplas (categoria, metrica, valor_num, valor_texto)
        conn:   conexión pyodbc activa
    """
    cursor = conn.cursor()
    
    # Limpiar resultados anteriores del mismo análisis
    cursor.execute("DELETE FROM RESULTADO_SPARK WHERE nombre_analisis = ?", nombre)
    
    sql = """
        INSERT INTO RESULTADO_SPARK (nombre_analisis, categoria, metrica, valor_numerico, valor_texto)
        VALUES (?, ?, ?, ?, ?)
    """
    
    for row in data:
        categoria, metrica, valor_num, valor_texto = row
        cursor.execute(sql, (nombre, categoria, metrica, valor_num, valor_texto))
    
    conn.commit()
    log.info(f"   Guardado '{nombre}': {len(data)} filas en RESULTADO_SPARK")


# =============================================================================
# ANÁLISIS SPARK
# =============================================================================

def analisis_A_salud_fiscal(df_contribuyente):
    """
    ANÁLISIS A: Salud fiscal del ecosistema comercial de Chimbote.
    
    Pregunta de negocio: ¿Cuántos contribuyentes están activos vs dados de baja?
    ¿Cuál es la deuda total acumulada? ¿Quiénes deben más?
    Relevancia para FAVISA: identifica competidores activos y evalúa riesgo
    de proveedores con deuda tributaria alta.
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS A: SALUD FISCAL DEL ECOSISTEMA")
    print("─"*55)
    
    # ── A1: Distribución por estado tributario ────────────────────────────────
    df_estado = (df_contribuyente
        .groupBy("id_estado")
        .agg(
            F.count("*").alias("cantidad"),
            F.sum("deuda").alias("deuda_total"),
            F.avg("deuda").alias("deuda_promedio")
        )
        .orderBy(F.col("cantidad").desc())
    )
    
    print("\nA1. Contribuyentes por estado tributario:")
    print(f"  {'Estado':<15} {'Cantidad':>10} {'Deuda Total':>15} {'Deuda Prom':>12}")
    print(f"  {'-'*15} {'-'*10} {'-'*15} {'-'*12}")
    
    resultados_A1 = []
    for row in df_estado.collect():
        estado = row['id_estado'] or 'ND'
        cant = row['cantidad']
        deuda_t = row['deuda_total'] or 0
        deuda_p = row['deuda_promedio'] or 0
        print(f"  {estado:<15} {cant:>10,}  S/ {deuda_t:>12,.2f}  S/ {deuda_p:>9,.2f}")
        resultados_A1.append((estado, 'cantidad', float(cant), str(cant)))
        resultados_A1.append((estado, 'deuda_total', float(deuda_t), f"S/ {deuda_t:,.2f}"))
    
    # ── A2: Estadísticas de deuda ─────────────────────────────────────────────
    df_deuda = df_contribuyente.filter(F.col("deuda") > 0)
    stats_deuda = df_deuda.agg(
        F.count("*").alias("con_deuda"),
        F.sum("deuda").alias("total"),
        F.avg("deuda").alias("promedio"),
        F.max("deuda").alias("maxima"),
        F.min("deuda").alias("minima"),
        F.expr("percentile_approx(deuda, 0.5)").alias("mediana"),
        F.expr("percentile_approx(deuda, 0.9)").alias("p90")
    ).collect()[0]
    
    total_contribuyentes = df_contribuyente.count()
    pct_con_deuda = (stats_deuda['con_deuda'] / total_contribuyentes * 100) if total_contribuyentes > 0 else 0
    
    print(f"\nA2. Estadísticas de deuda tributaria:")
    print(f"  Con deuda:          {stats_deuda['con_deuda']:>8,}  ({pct_con_deuda:.1f}% del total)")
    print(f"  Deuda total:      S/ {stats_deuda['total']:>12,.2f}")
    print(f"  Deuda promedio:   S/ {stats_deuda['promedio']:>12,.2f}")
    print(f"  Deuda mediana:    S/ {stats_deuda['mediana']:>12,.2f}")
    print(f"  Percentil 90:     S/ {stats_deuda['p90']:>12,.2f}")
    print(f"  Deuda máxima:     S/ {stats_deuda['maxima']:>12,.2f}")
    
    resultados_A2 = [
        ('DEUDA', 'con_deuda',      float(stats_deuda['con_deuda']),   f"{stats_deuda['con_deuda']:,}"),
        ('DEUDA', 'deuda_total',    float(stats_deuda['total'] or 0),  f"S/ {stats_deuda['total'] or 0:,.2f}"),
        ('DEUDA', 'deuda_promedio', float(stats_deuda['promedio'] or 0), f"S/ {stats_deuda['promedio'] or 0:,.2f}"),
        ('DEUDA', 'deuda_mediana',  float(stats_deuda['mediana'] or 0), f"S/ {stats_deuda['mediana'] or 0:,.2f}"),
        ('DEUDA', 'deuda_maxima',   float(stats_deuda['maxima'] or 0), f"S/ {stats_deuda['maxima'] or 0:,.2f}"),
    ]
    
    return {
        'A1_estado': resultados_A1,
        'A2_deuda':  resultados_A2
    }


def analisis_B_geografia(df_contribuyente):
    """
    ANÁLISIS B: Distribución geográfica de contribuyentes.
    
    Pregunta de negocio: ¿En qué distritos se concentra la actividad comercial?
    Relevancia para FAVISA: identifica mercados potenciales para expansión
    de CHIC, eventos y distribución de conservas.
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS B: DISTRIBUCIÓN GEOGRÁFICA")
    print("─"*55)
    
    df_geo = (df_contribuyente
        .groupBy("id_ubicacion")
        .agg(
            F.count("*").alias("empresas"),
            F.sum(F.when(F.col("id_estado") == "ACTIVO", 1).otherwise(0)).alias("activos"),
            F.sum("deuda").alias("deuda_total"),
            F.countDistinct("id_ciiu").alias("sectores_distintos")
        )
        .orderBy(F.col("empresas").desc())
        .limit(15)
    )
    
    print("\nB1. Top 15 ubicaciones por concentración empresarial:")
    print(f"  {'Ubicación':<25} {'Total':>7} {'Activos':>8} {'Sectores':>9} {'Deuda Total':>14}")
    print(f"  {'-'*25} {'-'*7} {'-'*8} {'-'*9} {'-'*14}")
    
    resultados = []
    for row in df_geo.collect():
        ubic = (row['id_ubicacion'] or 'ND')[:24]
        total = row['empresas']
        activos = row['activos'] or 0
        sectores = row['sectores_distintos']
        deuda = row['deuda_total'] or 0
        print(f"  {ubic:<25} {total:>7,} {activos:>8,} {sectores:>9,}  S/ {deuda:>10,.0f}")
        resultados.append((ubic, 'empresas', float(total), str(total)))
        resultados.append((ubic, 'deuda_total', float(deuda), f"S/ {deuda:,.2f}"))
    
    return {'B1_geo': resultados}


def analisis_C_estructura_empresarial(df_contribuyente):
    """
    ANÁLISIS C: Estructura del tejido empresarial local.
    
    Pregunta de negocio: ¿Predominan las micro o medianas empresas?
    ¿Qué tipo de personería jurídica es más común?
    Relevancia: FAVISA compite/colabora con este tejido empresarial.
    Conocer su composición ayuda a definir estrategias de precios y alianzas.
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS C: ESTRUCTURA EMPRESARIAL")
    print("─"*55)
    
    # ── C1: Por tamaño de empresa ──────────────────────────────────────────────
    df_tamano = (df_contribuyente
        .groupBy("id_tamano")
        .agg(
            F.count("*").alias("cantidad"),
            F.sum("deuda").alias("deuda_total"),
            F.avg("edad").alias("edad_promedio")
        )
        .orderBy(F.col("cantidad").desc())
    )
    
    total = df_contribuyente.count()
    print("\nC1. Por tamaño de empresa:")
    print(f"  {'Tamaño':<8} {'Cantidad':>10} {'%':>6} {'Deuda Total':>14}")
    print(f"  {'-'*8} {'-'*10} {'-'*6} {'-'*14}")
    
    resultados_C1 = []
    for row in df_tamano.collect():
        tam = row['id_tamano'] or '?'
        map_desc = {'C': 'Micro/Peq', 'M': 'Mediana', 'G': 'Grande', 'B': 'Básica'}
        desc = map_desc.get(tam, tam)
        cant = row['cantidad']
        pct = cant / total * 100 if total > 0 else 0
        deuda = row['deuda_total'] or 0
        print(f"  {desc:<8} {cant:>10,}  {pct:>5.1f}%  S/ {deuda:>10,.2f}")
        resultados_C1.append((desc, 'cantidad', float(cant), f"{cant:,} ({pct:.1f}%)"))
    
    # ── C2: Por tipo de personería ──────────────────────────────────────────────
    df_tipo = (df_contribuyente
        .groupBy("id_tipo_empresa")
        .agg(F.count("*").alias("cantidad"))
        .orderBy(F.col("cantidad").desc())
    )
    
    print("\nC2. Por tipo de personería:")
    map_tipo = {'A': 'Persona Natural', 'B': 'Persona Jurídica', 'CE': 'Cooperativa', 
                'C': 'Cooperativa', 'D': 'EIRL', 'E': 'Empresa Estado', '-': 'Sin definir'}
    
    resultados_C2 = []
    for row in df_tipo.collect():
        tipo = row['id_tipo_empresa'] or '?'
        desc = map_tipo.get(tipo, tipo)
        cant = row['cantidad']
        pct = cant / total * 100 if total > 0 else 0
        print(f"  {desc:<25} {cant:>8,}  ({pct:.1f}%)")
        resultados_C2.append((desc, 'cantidad', float(cant), f"{cant:,} ({pct:.1f}%)"))
    
    return {'C1_tamano': resultados_C1, 'C2_tipo': resultados_C2}


def analisis_D_sectores_economicos(df_contribuyente):
    """
    ANÁLISIS D: Sectores económicos (CIIU) más relevantes para FAVISA.
    
    Los CIIU de interés para FAVISA son:
    - 52xx: Comercio minorista (competidores directos de abarrotes)
    - 55xx: Hoteles y restaurantes (clientes potenciales del salón de eventos)
    - 15xx: Industria de alimentos y bebidas (incluye conservas)
    - 75xx: Actividades empresariales (contexto general)
    
    Pregunta: ¿Cuántos competidores directos hay activos? ¿Cuánta deuda acumulan?
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS D: SECTORES ECONÓMICOS (TOP CIIU)")
    print("─"*55)
    
    # Top 20 sectores por cantidad de empresas
    df_ciiu = (df_contribuyente
        .groupBy("id_ciiu")
        .agg(
            F.count("*").alias("empresas"),
            F.sum(F.when(F.col("id_estado") == "ACTIVO", 1).otherwise(0)).alias("activos"),
            F.sum("deuda").alias("deuda_total")
        )
        .orderBy(F.col("empresas").desc())
        .limit(20)
    )
    
    print("\nD1. Top 20 actividades económicas (por número de empresas):")
    print(f"  {'CIIU':<8} {'Total':>7} {'Activos':>8} {'Deuda Total':>14}")
    print(f"  {'-'*8} {'-'*7} {'-'*8} {'-'*14}")
    
    resultados_D = []
    for row in df_ciiu.collect():
        ciiu = row['id_ciiu'] or 'ND'
        total = row['empresas']
        activos = row['activos'] or 0
        deuda = row['deuda_total'] or 0
        print(f"  {ciiu:<8} {total:>7,} {activos:>8,}  S/ {deuda:>10,.2f}")
        resultados_D.append((ciiu, 'empresas', float(total), str(total)))
    
    # Análisis específico: sector de abarrotes/comercio (CIIU 52xx)
    df_abarrotes = df_contribuyente.filter(F.col("id_ciiu").startswith("52"))
    total_abarrotes = df_abarrotes.count()
    activos_abarrotes = df_abarrotes.filter(F.col("id_estado") == "ACTIVO").count()
    
    print(f"\nD2. Sector comercio minorista (CIIU 52xx) - Competidores FAVISA Abarrotes:")
    print(f"  Total empresas sector 52xx:    {total_abarrotes:,}")
    print(f"  Activos:                       {activos_abarrotes:,}")
    
    # Sector conservas (CIIU 15xx)
    df_conservas = df_contribuyente.filter(F.col("id_ciiu").startswith("15"))
    total_conservas = df_conservas.count()
    print(f"\nD3. Sector industria alimentaria (CIIU 15xx) - Conservas de pescado:")
    print(f"  Total empresas sector 15xx:    {total_conservas:,}")
    
    # Sector eventos / inmobiliario (CIIU 70xx, 55xx)
    df_eventos = df_contribuyente.filter(
        F.col("id_ciiu").startswith("70") | F.col("id_ciiu").startswith("55")
    )
    total_eventos = df_eventos.count()
    print(f"\nD4. Sector eventos/inmobiliario (CIIU 70xx + 55xx) - Galería CHIC / Salón:")
    print(f"  Total empresas sectores:       {total_eventos:,}")
    
    resultados_D.extend([
        ('SECTOR_52xx', 'total', float(total_abarrotes), f"{total_abarrotes:,}"),
        ('SECTOR_52xx', 'activos', float(activos_abarrotes), f"{activos_abarrotes:,}"),
        ('SECTOR_15xx', 'total', float(total_conservas), f"{total_conservas:,}"),
        ('SECTOR_70_55xx', 'total', float(total_eventos), f"{total_eventos:,}"),
    ])
    
    return {'D_sectores': resultados_D}


def analisis_E_demografia(df_contribuyente):
    """
    ANÁLISIS E: Demografía de propietarios/contribuyentes.
    
    Pregunta: ¿Cuál es la distribución por sexo y edad?
    Relevancia: FAVISA puede segmentar sus eventos y productos según demografía.
    También útil para el salón de eventos (matrimonios, quince años, etc.)
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS E: DEMOGRAFÍA DE PROPIETARIOS")
    print("─"*55)
    
    # ── E1: Por sexo ──────────────────────────────────────────────────────────
    df_sexo = (df_contribuyente
        .groupBy("sexo")
        .agg(
            F.count("*").alias("cantidad"),
            F.avg("deuda").alias("deuda_promedio"),
            F.avg("edad").alias("edad_promedio")
        )
        .orderBy(F.col("cantidad").desc())
    )
    
    total = df_contribuyente.count()
    print("\nE1. Distribución por sexo:")
    
    resultados_E = []
    for row in df_sexo.collect():
        sexo = row['sexo'] or 'ND'
        cant = row['cantidad']
        pct = cant / total * 100 if total > 0 else 0
        edad_p = row['edad_promedio'] or 0
        deuda_p = row['deuda_promedio'] or 0
        print(f"  {sexo:<10} {cant:>8,}  ({pct:>5.1f}%)  Edad prom: {edad_p:.1f}  Deuda prom: S/ {deuda_p:.2f}")
        resultados_E.append((sexo, 'cantidad', float(cant), f"{cant:,} ({pct:.1f}%)"))
        resultados_E.append((sexo, 'edad_promedio', float(edad_p), f"{edad_p:.1f} años"))
    
    # ── E2: Distribución por rango de edad ───────────────────────────────────
    df_edad = df_contribuyente.filter(F.col("edad").isNotNull() & (F.col("edad") > 0))
    
    df_rango = df_edad.withColumn(
        "rango_edad",
        F.when(F.col("edad") < 30, "< 30 años")
         .when(F.col("edad") < 40, "30-39 años")
         .when(F.col("edad") < 50, "40-49 años")
         .when(F.col("edad") < 60, "50-59 años")
         .when(F.col("edad") < 70, "60-69 años")
         .otherwise("70+ años")
    ).groupBy("rango_edad").agg(F.count("*").alias("cantidad")).orderBy("rango_edad")
    
    print("\nE2. Distribución por rango de edad:")
    for row in df_rango.collect():
        rango = row['rango_edad']
        cant = row['cantidad']
        bar = "█" * int(cant / total * 200)  # barra visual proporcional
        print(f"  {rango:<12} {cant:>8,}  {bar[:30]}")
        resultados_E.append((rango, 'cantidad', float(cant), str(cant)))
    
    return {'E_demografia': resultados_E}


def analisis_F_limpieza_datos(df_contribuyente):
    """
    ANÁLISIS F: Calidad y limpieza de los datos.
    
    Demuestra el uso de Spark para diagnóstico de calidad de datos,
    algo fundamental en cualquier pipeline de datos empresarial.
    Identifica los datos sucios que se encontraron en el CSV original.
    """
    print("\n" + "─"*55)
    print("  ANÁLISIS F: DIAGNÓSTICO DE CALIDAD DE DATOS")
    print("─"*55)
    
    total = df_contribuyente.count()
    
    # Contar nulos por columna
    nulos = {}
    for col in ["edad", "deuda", "id_ciiu", "id_tipo_empresa", "id_estado", "id_condicion"]:
        nulos[col] = df_contribuyente.filter(F.col(col).isNull()).count()
    
    print("\nF1. Valores nulos por columna:")
    resultados_F = []
    for col, n_nulos in nulos.items():
        pct = n_nulos / total * 100 if total > 0 else 0
        estado = "✅" if pct < 1 else ("⚠️" if pct < 10 else "❌")
        print(f"  {estado} {col:<20} {n_nulos:>8,}  ({pct:>5.1f}%)")
        resultados_F.append((col, 'nulos', float(n_nulos), f"{n_nulos:,} ({pct:.1f}%)"))
    
    # Contribuyentes con condición "ND" (datos no determinados = sucios)
    nd_cond = df_contribuyente.filter(F.col("id_condicion") == "ND").count()
    nd_estado = df_contribuyente.filter(F.col("id_estado") == "ND").count()
    
    print(f"\nF2. Registros con datos normalizados (era sucio en CSV original):")
    print(f"  Estado tributario 'ND' (era '2ACTIVO' etc.): {nd_estado:,}")
    print(f"  Condición domicilio 'ND' (era '2HABIDO' etc.): {nd_cond:,}")
    print(f"  Tipo empresa '-' (era 'A7','B9' etc.):  calculando...")
    
    sin_tipo = df_contribuyente.filter(F.col("id_tipo_empresa") == "-").count()
    print(f"    Tipo empresa normalizado a '-': {sin_tipo:,}")
    
    resultados_F.extend([
        ('CALIDAD', 'nd_estado',   float(nd_estado),  f"{nd_estado:,}"),
        ('CALIDAD', 'nd_condicion', float(nd_cond),    f"{nd_cond:,}"),
        ('CALIDAD', 'sin_tipo',    float(sin_tipo),    f"{sin_tipo:,}"),
        ('CALIDAD', 'total_limpios', float(total - nd_estado - nd_cond), 
         f"{total - nd_estado - nd_cond:,}"),
    ])
    
    return {'F_calidad': resultados_F}


# =============================================================================
# FUNCIÓN PRINCIPAL DEL JOB SPARK
# =============================================================================

def run():
    """
    Función principal que coordina todos los análisis.
    Es invocada desde main.py.
    """
    spark = None
    conn = None
    
    try:
        # ── Crear sesión Spark ───────────────────────────────────────────────
        spark = crear_spark_session()
        
        # ── Leer datos desde SQL Server ──────────────────────────────────────
        log.info("Leyendo datos desde SQL Server...")
        df = leer_tabla_spark(spark, "CONTRIBUYENTE")
        
        # Cachear el DataFrame principal: se usará en múltiples análisis
        # sin volver a leer de SQL Server cada vez.
        df.cache()
        df.count()  # Forzar materialización del caché
        log.info("DataFrame cacheado en memoria de Spark ✓")
        
        # ── Ejecutar análisis ────────────────────────────────────────────────
        print("\n" + "="*55)
        print("  ANÁLISIS SPARK - FAVISA CONTRIBUYENTES CHIMBOTE")
        print("="*55)
        
        res_A = analisis_A_salud_fiscal(df)
        res_B = analisis_B_geografia(df)
        res_C = analisis_C_estructura_empresarial(df)
        res_D = analisis_D_sectores_economicos(df)
        res_E = analisis_E_demografia(df)
        res_F = analisis_F_limpieza_datos(df)
        
        # ── Guardar resultados en SQL Server ─────────────────────────────────
        log.info("\nGuardando resultados en RESULTADO_SPARK...")
        conn = get_connection()
        
        guardar_resultado_spark("Salud Fiscal - Por Estado",     res_A['A1_estado'], conn)
        guardar_resultado_spark("Salud Fiscal - Estadísticas Deuda", res_A['A2_deuda'], conn)
        guardar_resultado_spark("Geografía - Por Ubicación",     res_B['B1_geo'], conn)
        guardar_resultado_spark("Estructura - Por Tamaño",       res_C['C1_tamano'], conn)
        guardar_resultado_spark("Estructura - Por Tipo Empresa", res_C['C2_tipo'], conn)
        guardar_resultado_spark("Sectores Económicos",           res_D['D_sectores'], conn)
        guardar_resultado_spark("Demografía",                    res_E['E_demografia'], conn)
        guardar_resultado_spark("Calidad de Datos",              res_F['F_calidad'], conn)
        
        print("\n" + "="*55)
        print("  ✅ ANÁLISIS SPARK COMPLETADO")
        print("  📊 Resultados guardados en RESULTADO_SPARK")
        print("  🌐 Ver dashboard en http://localhost:8501")
        print("  ⚡ Ver Spark UI en http://localhost:8080")
        print("="*55 + "\n")
        
    except Exception as e:
        log.error(f"Error en spark_job.run(): {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Siempre cerrar la sesión Spark para liberar recursos
        if spark:
            spark.stop()
            log.info("Spark Session cerrada.")
        if conn:
            conn.close()


# Permite ejecutar el job directamente: python spark_job.py
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    run()
