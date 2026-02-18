# =============================================================================
# dashboard.py - DASHBOARD STREAMLIT PARA FAVISA
# =============================================================================
# Interfaz web simple y académica para visualizar los resultados del análisis.
# Se ejecuta automáticamente después de main.py (ver docker-compose.yml).
#
# Acceso: http://localhost:8501
#
# No requiere tocar el código para ver los datos;
# el botón "Actualizar / Re-ejecutar análisis" lanza main.py de nuevo.
# =============================================================================

import streamlit as st
import pandas as pd
import sys
import subprocess

sys.path.insert(0, '/app/python')
from db_connection import get_connection

# ─── CONFIGURACIÓN DE LA PÁGINA ───────────────────────────────────────────────
st.set_page_config(
    page_title="FAVISA - Análisis de Contribuyentes",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── ESTILO CSS BÁSICO ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card { background: #f0f2f6; border-radius: 8px; padding: 15px; margin: 5px; }
    .titulo-seccion { color: #1f4e79; border-bottom: 2px solid #1f4e79; padding-bottom: 5px; }
    .stButton>button { width: 100%; background-color: #1f4e79; color: white; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# FUNCIONES DE CARGA DE DATOS
# =============================================================================

@st.cache_data(ttl=60)  # Caché por 60 segundos
def cargar_datos_sql(query: str) -> pd.DataFrame:
    """Ejecuta una query en SQL Server y devuelve un DataFrame de Pandas."""
    try:
        conn = get_connection(timeout=30)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error conectando a SQL Server: {e}")
        return pd.DataFrame()


def cargar_resumen_general():
    return cargar_datos_sql("""
        SELECT 
            COUNT(*) as total_contribuyentes,
            SUM(CASE WHEN id_estado = 'ACTIVO' THEN 1 ELSE 0 END) as activos,
            SUM(CASE WHEN deuda > 0 THEN 1 ELSE 0 END) as con_deuda,
            SUM(deuda) as deuda_total,
            AVG(deuda) as deuda_promedio,
            MAX(deuda) as deuda_maxima
        FROM CONTRIBUYENTE
    """)


def cargar_resultados_spark(nombre_analisis: str):
    return cargar_datos_sql(f"""
        SELECT categoria, metrica, valor_numerico, valor_texto, fecha_ejecucion
        FROM RESULTADO_SPARK
        WHERE nombre_analisis = '{nombre_analisis}'
        ORDER BY valor_numerico DESC
    """)


def cargar_por_estado():
    return cargar_datos_sql("""
        SELECT 
            et.descripcion as estado,
            COUNT(*) as cantidad,
            SUM(c.deuda) as deuda_total
        FROM CONTRIBUYENTE c
        JOIN ESTADO_TRIBUTARIO et ON c.id_estado = et.id_estado
        GROUP BY et.descripcion
        ORDER BY cantidad DESC
    """)


def cargar_por_ubicacion():
    return cargar_datos_sql("""
        SELECT TOP 15
            ug.nombre_distrito as ubicacion,
            COUNT(*) as empresas,
            SUM(c.deuda) as deuda_total
        FROM CONTRIBUYENTE c
        JOIN UBICACION_GEOGRAFICA ug ON c.id_ubicacion = ug.id_ubicacion
        GROUP BY ug.nombre_distrito
        ORDER BY empresas DESC
    """)


def cargar_por_tamano():
    return cargar_datos_sql("""
        SELECT
            tm.descripcion as tamano,
            COUNT(*) as cantidad,
            AVG(c.deuda) as deuda_promedio
        FROM CONTRIBUYENTE c
        JOIN TAMANO_EMPRESA tm ON c.id_tamano = tm.id_tamano
        GROUP BY tm.descripcion
        ORDER BY cantidad DESC
    """)


def cargar_por_sexo():
    return cargar_datos_sql("""
        SELECT 
            sexo,
            COUNT(*) as cantidad,
            AVG(edad) as edad_promedio,
            SUM(deuda) as deuda_total
        FROM CONTRIBUYENTE
        GROUP BY sexo
        ORDER BY cantidad DESC
    """)


def cargar_top_ciiu():
    return cargar_datos_sql("""
        SELECT TOP 15
            ae.id_ciiu as ciiu,
            ae.descripcion as actividad,
            COUNT(*) as empresas,
            SUM(c.deuda) as deuda_total
        FROM CONTRIBUYENTE c
        JOIN ACTIVIDAD_ECONOMICA ae ON c.id_ciiu = ae.id_ciiu
        GROUP BY ae.id_ciiu, ae.descripcion
        ORDER BY empresas DESC
    """)


# =============================================================================
# LAYOUT PRINCIPAL DEL DASHBOARD
# =============================================================================

# ── ENCABEZADO ────────────────────────────────────────────────────────────────
st.title("📊 Sistema FAVISA - Análisis de Contribuyentes Chimbote")
st.markdown("""
**Universidad Nacional del Santa** | Arquitectura de Software Empresarial | Grupo B  
*Integración SQL Server + Apache Spark + Python en Docker*
""")
st.markdown("---")

# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Control del Sistema")
    
    st.markdown("""
    **Tecnologías:**
    - 🗄️ SQL Server 2022 (Docker)
    - ⚡ Apache Spark 3.5
    - 🐍 Python + PySpark
    - 📊 Streamlit
    """)
    
    st.markdown("---")
    
    if st.button("🔄 Re-ejecutar Análisis Spark"):
        with st.spinner("Ejecutando análisis completo..."):
            result = subprocess.run(
                ["python", "/app/python/main.py"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                st.success("✅ Análisis completado")
                st.cache_data.clear()
            else:
                st.error(f"❌ Error: {result.stderr[-500:]}")
    
    st.markdown("---")
    st.markdown("""
    **Links útiles:**
    - [Spark UI :8080](http://localhost:8080)
    - [Spark Worker :8081](http://localhost:8081)
    """)
    
    st.markdown("---")
    st.caption("💡 Datos: DataOZ Chimbote ~50K registros")

# ── MÉTRICAS PRINCIPALES ──────────────────────────────────────────────────────
st.markdown('<h3 class="titulo-seccion">📈 Métricas Principales</h3>', unsafe_allow_html=True)

df_resumen = cargar_resumen_general()
if not df_resumen.empty:
    r = df_resumen.iloc[0]
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Contribuyentes", f"{int(r['total_contribuyentes']):,}")
    with col2:
        pct_activos = int(r['activos']) / int(r['total_contribuyentes']) * 100 if r['total_contribuyentes'] > 0 else 0
        st.metric("Activos", f"{int(r['activos']):,}", f"{pct_activos:.1f}%")
    with col3:
        st.metric("Con Deuda", f"{int(r['con_deuda']):,}")
    with col4:
        st.metric("Deuda Total", f"S/ {float(r['deuda_total'] or 0):,.0f}")
    with col5:
        st.metric("Deuda Promedio", f"S/ {float(r['deuda_promedio'] or 0):,.2f}")

st.markdown("---")

# ── FILA 1: ESTADO TRIBUTARIO + DISTRIBUCIÓN GEOGRÁFICA ───────────────────────
col_izq, col_der = st.columns(2)

with col_izq:
    st.markdown('<h4 class="titulo-seccion">📋 Estado Tributario</h4>', unsafe_allow_html=True)
    df_estado = cargar_por_estado()
    if not df_estado.empty:
        st.bar_chart(df_estado.set_index('estado')['cantidad'])
        st.dataframe(df_estado, use_container_width=True, hide_index=True)

with col_der:
    st.markdown('<h4 class="titulo-seccion">📍 Top 15 Ubicaciones</h4>', unsafe_allow_html=True)
    df_ubic = cargar_por_ubicacion()
    if not df_ubic.empty:
        st.bar_chart(df_ubic.set_index('ubicacion')['empresas'])
        st.dataframe(df_ubic, use_container_width=True, hide_index=True)

st.markdown("---")

# ── FILA 2: DEMOGRAFÍA + TAMAÑO EMPRESA ───────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.markdown('<h4 class="titulo-seccion">👥 Demografía (Sexo)</h4>', unsafe_allow_html=True)
    df_sexo = cargar_por_sexo()
    if not df_sexo.empty:
        st.bar_chart(df_sexo.set_index('sexo')['cantidad'])
        st.dataframe(df_sexo, use_container_width=True, hide_index=True)

with col_b:
    st.markdown('<h4 class="titulo-seccion">🏢 Tamaño de Empresa</h4>', unsafe_allow_html=True)
    df_tamano = cargar_por_tamano()
    if not df_tamano.empty:
        st.bar_chart(df_tamano.set_index('tamano')['cantidad'])
        st.dataframe(df_tamano, use_container_width=True, hide_index=True)

st.markdown("---")

# ── FILA 3: TOP CIIU ──────────────────────────────────────────────────────────
st.markdown('<h4 class="titulo-seccion">🏭 Top 15 Actividades Económicas (CIIU)</h4>', unsafe_allow_html=True)
df_ciiu = cargar_top_ciiu()
if not df_ciiu.empty:
    st.bar_chart(df_ciiu.set_index('ciiu')['empresas'])
    st.dataframe(df_ciiu, use_container_width=True, hide_index=True)

st.markdown("---")

# ── RESULTADOS SPARK ──────────────────────────────────────────────────────────
st.markdown('<h4 class="titulo-seccion">⚡ Resultados Análisis Apache Spark</h4>', unsafe_allow_html=True)

analisis_disponibles = [
    "Salud Fiscal - Por Estado",
    "Salud Fiscal - Estadísticas Deuda", 
    "Geografía - Por Ubicación",
    "Estructura - Por Tamaño",
    "Sectores Económicos",
    "Demografía",
    "Calidad de Datos"
]

analisis_sel = st.selectbox("Seleccionar análisis:", analisis_disponibles)

df_spark = cargar_resultados_spark(analisis_sel)
if not df_spark.empty:
    st.dataframe(df_spark[['categoria', 'metrica', 'valor_numerico', 'valor_texto']],
                 use_container_width=True, hide_index=True)
    
    if 'valor_numerico' in df_spark.columns and not df_spark['valor_numerico'].isna().all():
        df_chart = df_spark[['categoria', 'metrica', 'valor_numerico']].dropna()
        if len(df_chart) > 0 and len(df_chart) <= 30:
            df_chart_pivot = df_chart.groupby('categoria')['valor_numerico'].first().reset_index()
            if len(df_chart_pivot) > 1:
                st.bar_chart(df_chart_pivot.set_index('categoria')['valor_numerico'])
else:
    st.info("No hay resultados Spark aún. Presiona 'Re-ejecutar Análisis Spark' en el sidebar.")

# ── PIE DE PÁGINA ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray; font-size: 12px;'>
    Sistema FAVISA | Universidad Nacional del Santa | Arquitectura de Software Empresarial 2026<br>
    SQL Server 🔗 Apache Spark 🔗 Python 🔗 Docker | Grupo B
</div>
""", unsafe_allow_html=True)
