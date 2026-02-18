# 📊 Sistema FAVISA - Análisis de Contribuyentes Chimbote

**Universidad Nacional del Santa | Arquitectura de Software Empresarial | Grupo B | 2026**

Demostración académica de integración **SQL Server + Apache Spark + Python** usando **Docker**.

---

### Flujo de ejecución:

1. **SQL Server** arranca y espera conexiones
2. **Python** espera a que SQL Server esté listo (healthcheck)
3. **Python** ejecuta `init.sql` → crea FAVISA_DB con todas las tablas
4. **Python** lee el CSV y lo carga via `pyodbc` (tabla staging)
5. **Python** limpia datos sucios y migra a tabla `CONTRIBUYENTE`
6. **PySpark** se conecta a SQL Server via JDBC y realiza 6 análisis
7. Los resultados se guardan en `RESULTADO_SPARK` y se muestran en consola
8. **Streamlit** lanza el dashboard web en el puerto 8501

---

## 📁 Estructura del Proyecto

```
proyecto/
├── docker/
│   ├── docker-compose.yml    # Orquestación de contenedores
│   └── Dockerfile            # Imagen Python personalizada
├── sql/
│   └── init.sql              # Script BD completo (adaptado para Docker)
├── python/
│   ├── main.py               # Orquestador principal
│   ├── spark_job.py          # Análisis con Apache Spark
│   ├── db_connection.py      # Módulo de conexión SQL Server
│   ├── dashboard.py          # Dashboard Streamlit
│   └── requirements.txt      # Dependencias Python
└── data/
    └── DataOZ_ChimboteNuevoCampo_03.csv   # Datos de contribuyentes
```

---

## ✅ Requisitos Previos

Solo necesitas **Docker Desktop** instalado:

| Sistema | Link                                                     |
| ------- | -------------------------------------------------------- |
| Windows | https://docs.docker.com/desktop/install/windows-install/ |
| Mac     | https://docs.docker.com/desktop/install/mac-install/     |
| Linux   | https://docs.docker.com/desktop/install/linux-install/   |

Verificar instalación:

```bash
docker --version
docker compose version
```

**Recursos recomendados para Docker Desktop:**

- RAM: mínimo 6 GB asignados a Docker (8 GB recomendado)
- CPU: 2 cores mínimo
- Disco: 10 GB libres

---

## 🚀 Ejecución Paso a Paso

### 1. Clonar / Descomprimir el proyecto

```bash
# Si tienes git:
git clone <repositorio>

# O simplemente descomprimir el ZIP y entrar al directorio
cd proyecto
```

### 2. Ejecutar el sistema

```bash
# Desde la carpeta /docker (donde está el docker-compose.yml):
cd docker

# Construir imágenes y levantar todos los contenedores:
docker compose up --build
```

> ⏱️ **Primera vez**: puede tardar 5-10 minutos descargando imágenes.
> Las siguientes ejecuciones tardan ~2 minutos.

### 3. Observar los logs

Verás en consola algo así:

```
favisa_sqlserver  | SQL Server is now ready for client connections.
favisa_python     | [PASO 1] Verificando conexión a SQL Server... ✅
favisa_python     | [PASO 2] Ejecutando script SQL...
favisa_python     | [PASO 3] Cargando CSV... 50,100 filas OK
favisa_python     | [PASO 4] Poblando catálogos...
favisa_python     | [PASO 5] Migrando datos...
favisa_python     | [PASO 6] Ejecutando análisis Spark...
favisa_python     |   ANÁLISIS A: SALUD FISCAL DEL ECOSISTEMA
favisa_python     |   ANÁLISIS B: DISTRIBUCIÓN GEOGRÁFICA
...
favisa_python     | Dashboard disponible en: http://localhost:8501
```

### 4. Acceder a las interfaces

| Interfaz                | URL                   | Descripción                         |
| ----------------------- | --------------------- | ----------------------------------- |
| **Dashboard principal** | http://localhost:8501 | Streamlit con gráficos y resultados |
| **Spark Master UI**     | http://localhost:8080 | Estado del cluster Spark            |
| **Spark Worker UI**     | http://localhost:8081 | Tareas en ejecución                 |

---

## 🔄 Re-ejecución

Para ejecutar nuevamente sin reconstruir:

```bash
docker compose down
docker compose up
```

Para reconstruir la imagen Python (si cambias requirements.txt o Dockerfile):

```bash
docker compose up --build
```

Para ver solo los logs de un servicio:

```bash
docker compose logs python-app
docker compose logs sqlserver
docker compose logs spark-master
```

---

## 🗄️ Conectarse a SQL Server desde SSMS / DBeaver

Si quieres explorar la base desde una herramienta externa:

| Campo         | Valor            |
| ------------- | ---------------- |
| Servidor      | `localhost,1433` |
| Autenticación | SQL Server       |
| Usuario       | `SA`             |
| Contraseña    | `FavisaDB2024!`  |
| Base de datos | `FAVISA_DB`      |

---

## 📊 Análisis Spark Incluidos

| Análisis             | Descripción                                    | Relevancia FAVISA       |
| -------------------- | ---------------------------------------------- | ----------------------- |
| **A. Salud Fiscal**  | Estado tributario, deuda total/promedio/máxima | Riesgo de proveedores   |
| **B. Geografía**     | Top 15 distritos por concentración             | Expansión CHIC/Eventos  |
| **C. Estructura**    | Tamaño y tipo de empresa                       | Perfil de competidores  |
| **D. Sectores CIIU** | Top 20 + análisis sectores 52xx, 15xx, 70xx    | Competencia directa     |
| **E. Demografía**    | Sexo, edad, rangos etarios                     | Segmentación de mercado |
| **F. Calidad**       | Datos nulos, valores sucios normalizados       | Diagnóstico de datos    |

---

## 🔧 Troubleshooting

### "python-app se desconecta antes de que SQL Server esté listo"

Normal en la primera ejecución. El sistema tiene reintentos automáticos
(cada 5 segundos, hasta 2 minutos). SQL Server tarda ~45-60s en arrancar.

### "Error JDBC: driver not found"

El JAR JDBC se descarga durante el build del Dockerfile. Si falla la descarga
(sin internet), puedes descargarlo manualmente:

```
https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar
```

Y colocarlo en: `/opt/spark-jars/` dentro del contenedor.

### "Puerto 1433 ya en uso"

Tienes SQL Server local corriendo. Cambia el port mapping en docker-compose.yml:

```yaml
ports:
  - "1434:1433" # cambia el primer número
```

### Ver qué hay en la BD sin SSMS:

```bash
docker exec -it favisa_sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U SA -P 'FavisaDB2024!' \
  -Q "SELECT COUNT(*) FROM FAVISA_DB.dbo.CONTRIBUYENTE" -No -C
```

---

## 🛑 Detener el sistema

```bash
# Detener contenedores (mantiene datos):
docker compose down

# Detener Y borrar todos los datos (limpieza total):
docker compose down -v
```

---

## 📝 Notas de Adaptación del Script SQL

El script `init.sql` es el script original del equipo con estas adaptaciones mínimas:

1. **Se removió `BULK INSERT`** → La carga del CSV la hace Python (más portable)
2. **Se agregaron valores al catálogo** de estados y condiciones que aparecen en el CSV real
3. **Se creó tabla `RESULTADO_SPARK`** para persistir los análisis (no existía en original)
4. **Sin cambios en**: tablas, vistas, relaciones, índices, constraints de la estructura original

---

_Proyecto académico - Universidad Nacional del Santa - 2026_
