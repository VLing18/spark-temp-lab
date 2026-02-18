FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Java es requerido por PySpark (Spark está escrito en JVM)
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    curl \
    gnupg \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Driver ODBC de Microsoft para que Python hable con SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY python/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Descargar JAR JDBC de SQL Server para que Spark pueda leer la BD
RUN mkdir -p /opt/spark-jars && \
    curl -L -o /opt/spark-jars/mssql-jdbc.jar \
    "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar"

ENV SPARK_CLASSPATH=/opt/spark-jars/mssql-jdbc.jar

COPY python/ /app/python/
COPY data/ /app/data/
COPY sql/ /app/sql/

EXPOSE 8501 4040