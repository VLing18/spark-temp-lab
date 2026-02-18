/*
==============================================================================
SCRIPT FINAL - BASE DE DATOS FAVISA
==============================================================================
Proyecto: Sistema de Gestión de Contribuyentes
Grupo: Grupo B
Universidad: Universidad Nacional del Santa
Curso: Arquitectura de Software Empresarial
Año: 2026

DESCRIPCIÓN:
- Base de datos normalizada (3FN)
- 7 tablas: 1 principal + 6 catálogos
- 50,100 registros de contribuyentes
- Modelo optimizado con índices

NOTA DE ADAPTACIÓN DOCKER (agregado por el ingeniero de datos):
- Se removió el BULK INSERT (la carga del CSV la hace Python con pyodbc
  para máxima compatibilidad con Docker y rutas multiplataforma)
- Se agregaron los catálogos de valores sucios detectados en el CSV real
  (tipos de empresa extendidos, estados y condiciones con códigos numéricos)
- Se agregó una tabla RESULTADO_SPARK para guardar los análisis de Spark
- Se verificó compatibilidad con SQL Server 2022 en Linux (imagen Docker)
==============================================================================
*/

-- ============================================================================
-- PASO 1: CREAR BASE DE DATOS
-- ============================================================================
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'FAVISA_DB')
BEGIN
    ALTER DATABASE FAVISA_DB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE FAVISA_DB;
END
GO

CREATE DATABASE FAVISA_DB
    COLLATE Latin1_General_CI_AS;   -- Collation que soporta caracteres latinos (ñ, acentos)
GO

USE FAVISA_DB;
GO

-- ============================================================================
-- PASO 2: CREAR TABLA TEMPORAL PARA IMPORTAR CSV
-- (La llenará Python, no BULK INSERT - más portable en Docker)
-- ============================================================================
CREATE TABLE TMP_DataOZ_Staging (
    ddp_numruc  BIGINT,
    ddp_ciiu    VARCHAR(10),
    ddp_tpoemp  VARCHAR(5),
    ddp_tamano  VARCHAR(5),
    ddp_ubigeo  VARCHAR(100),
    ddp_estado  VARCHAR(50),
    ddp_flag22  VARCHAR(50),
    dds_sexo    VARCHAR(20),
    dds_edad    INT,
    deuda       DECIMAL(18,2),
    condicion   INT
);
GO

-- ============================================================================
-- PASO 3: CREAR TABLAS CATÁLOGO
-- ============================================================================

CREATE TABLE ACTIVIDAD_ECONOMICA (
    id_ciiu     VARCHAR(10)  NOT NULL,
    descripcion VARCHAR(500) NOT NULL DEFAULT 'Descripción pendiente',
    seccion     CHAR(1)      NULL,
    division    VARCHAR(2)   NULL,
    CONSTRAINT PK_ACTIVIDAD_ECONOMICA PRIMARY KEY (id_ciiu)
);
GO

CREATE TABLE TIPO_EMPRESA (
    id_tipo_empresa VARCHAR(5)   NOT NULL,
    descripcion     VARCHAR(200) NOT NULL DEFAULT 'Descripción pendiente',
    abreviatura     VARCHAR(10)  NULL,
    CONSTRAINT PK_TIPO_EMPRESA PRIMARY KEY (id_tipo_empresa)
);
GO

CREATE TABLE TAMANO_EMPRESA (
    id_tamano       VARCHAR(5)   NOT NULL,
    descripcion     VARCHAR(200) NOT NULL DEFAULT 'Descripción pendiente',
    criterio_ventas VARCHAR(100) NULL,
    CONSTRAINT PK_TAMANO_EMPRESA PRIMARY KEY (id_tamano)
);
GO

CREATE TABLE UBICACION_GEOGRAFICA (
    id_ubicacion    VARCHAR(100) NOT NULL,
    nombre_distrito VARCHAR(100) NOT NULL,
    provincia       VARCHAR(100) NULL DEFAULT 'Santa',
    departamento    VARCHAR(100) NULL DEFAULT 'Áncash',
    ubigeo_codigo   VARCHAR(6)   NULL,
    CONSTRAINT PK_UBICACION_GEOGRAFICA PRIMARY KEY (id_ubicacion)
);
GO

CREATE TABLE ESTADO_TRIBUTARIO (
    id_estado   VARCHAR(50)  NOT NULL,
    descripcion VARCHAR(200) NOT NULL DEFAULT 'Descripción pendiente',
    CONSTRAINT PK_ESTADO_TRIBUTARIO PRIMARY KEY (id_estado)
);
GO

CREATE TABLE CONDICION_DOMICILIO (
    id_condicion VARCHAR(50)  NOT NULL,
    descripcion  VARCHAR(200) NOT NULL DEFAULT 'Descripción pendiente',
    CONSTRAINT PK_CONDICION_DOMICILIO PRIMARY KEY (id_condicion)
);
GO

-- ============================================================================
-- PASO 4: POBLAR TABLAS CATÁLOGO CON VALORES CONOCIDOS
-- (Se poblarán con más valores desde Python al leer el CSV)
-- ============================================================================

-- CATÁLOGO: Tamaños de empresa
-- C = Chico/Micro/Pequeña, B = (valor detectado en CSV)
INSERT INTO TAMANO_EMPRESA (id_tamano, descripcion, criterio_ventas) VALUES
    ('C', 'Pequeña Empresa / Microempresa',  'Ventas anuales ≤ 1700 UIT'),
    ('M', 'Mediana Empresa',                  'Ventas anuales > 1700 UIT y ≤ 2300 UIT'),
    ('G', 'Grande Empresa',                   'Ventas anuales > 2300 UIT'),
    ('B', 'Empresa Básica / Sin clasificar',  'Sin criterio definido');
GO

-- CATÁLOGO: Tipos de empresa (valores limpios conocidos)
INSERT INTO TIPO_EMPRESA (id_tipo_empresa, descripcion, abreviatura) VALUES
    ('A',  'Persona Natural con Negocio',               'PN'),
    ('B',  'Persona Jurídica - Sociedad',                'PJ'),
    ('CE', 'Cooperativa Especial',                       'CE'),
    ('C',  'Cooperativa',                                'CO'),
    ('D',  'Empresa Individual de Responsabilidad Ltda','EIRL'),
    ('E',  'Empresa del Estado',                        'EE'),
    ('-',  'Sin tipo definido',                         'SD');
GO
-- NOTA AGREGADA (Docker): Los valores sucios del CSV como 'A7','B9','AA','AB',etc.
-- son datos corruptos detectados en el análisis. Se normalizan en Python
-- antes de insertar en la tabla principal.

-- CATÁLOGO: Estados tributarios
INSERT INTO ESTADO_TRIBUTARIO (id_estado, descripcion) VALUES
    ('ACTIVO',   'Contribuyente Activo'),
    ('10',       'Baja Temporal'),
    ('11',       'Baja Definitiva'),
    ('3',        'Suspendido Temporalmente'),
    ('2',        'Baja de Oficio'),
    ('1',        'Inhabilitado'),
    ('12',       'Baja Provisional'),
    ('INACTIVO', 'Contribuyente Inactivo'),
    ('ND',       'Estado No Determinado');
GO
-- NOTA AGREGADA (Docker): '2ACTIVO' detectado en CSV → dato corrupto,
-- se normaliza a 'ACTIVO' en Python antes de insertar.

-- CATÁLOGO: Condición de domicilio
INSERT INTO CONDICION_DOMICILIO (id_condicion, descripcion) VALUES
    ('HABIDO',  'Domicilio Fiscal Habido (Verificado)'),
    ('12',      'No Habido - Dirección Inexistente'),
    ('1',       'Pendiente de Verificación'),
    ('5',       'No Hallado'),
    ('2',       'No Existe la Dirección'),
    ('3',       'Cerrado'),
    ('4',       'Ausente'),
    ('6',       'No Habido - Dirección Desconocida'),
    ('7',       'No Habido - Rehúsa Recibir'),
    ('8',       'No Habido - Temporalmente Ausente'),
    ('9',       'No Habido - Otros Motivos'),
    ('10',      'No Habido - Mudado sin Informar'),
    ('11',      'No Habido - Local Cerrado'),
    ('ND',      'Condición No Determinada');
GO
-- NOTA AGREGADA (Docker): '2HABIDO' detectado en CSV → dato corrupto,
-- se normaliza a 'HABIDO' en Python antes de insertar.

-- ============================================================================
-- PASO 5: CREAR TABLA PRINCIPAL - CONTRIBUYENTE
-- (Estructura original sin cambios - solo se ajustó el CHECK de RUC)
-- ============================================================================
CREATE TABLE CONTRIBUYENTE (
    ruc             BIGINT       NOT NULL,
    id_ciiu         VARCHAR(10)  NOT NULL,
    id_tipo_empresa VARCHAR(5)   NOT NULL,
    id_tamano       VARCHAR(5)   NOT NULL,
    id_ubicacion    VARCHAR(100) NOT NULL,
    id_estado       VARCHAR(50)  NOT NULL,
    id_condicion    VARCHAR(50)  NOT NULL,
    sexo            VARCHAR(20)  NOT NULL,
    edad            INT          NULL,
    deuda           DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    flag22          VARCHAR(50)  NULL,

    CONSTRAINT PK_CONTRIBUYENTE PRIMARY KEY (ruc),

    CONSTRAINT FK_CONTRIBUYENTE_CIIU
        FOREIGN KEY (id_ciiu) REFERENCES ACTIVIDAD_ECONOMICA(id_ciiu)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT FK_CONTRIBUYENTE_TIPO
        FOREIGN KEY (id_tipo_empresa) REFERENCES TIPO_EMPRESA(id_tipo_empresa)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT FK_CONTRIBUYENTE_TAMANO
        FOREIGN KEY (id_tamano) REFERENCES TAMANO_EMPRESA(id_tamano)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT FK_CONTRIBUYENTE_UBICACION
        FOREIGN KEY (id_ubicacion) REFERENCES UBICACION_GEOGRAFICA(id_ubicacion)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT FK_CONTRIBUYENTE_ESTADO
        FOREIGN KEY (id_estado) REFERENCES ESTADO_TRIBUTARIO(id_estado)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT FK_CONTRIBUYENTE_CONDICION
        FOREIGN KEY (id_condicion) REFERENCES CONDICION_DOMICILIO(id_condicion)
        ON UPDATE CASCADE ON DELETE NO ACTION,

    CONSTRAINT CHK_SEXO_VALIDO
        CHECK (sexo IN ('HOMBRE', 'MUJER', 'ND')),

    CONSTRAINT CHK_EDAD_VALIDA
        CHECK (edad IS NULL OR (edad >= 0 AND edad <= 120)),

    CONSTRAINT CHK_DEUDA_NO_NEGATIVA
        CHECK (deuda >= 0),

    -- NOTA ADAPTADA (Docker): Acepta cualquier RUC positivo
    -- (el script original manejaba un RUC de 6 dígitos especial)
    CONSTRAINT CHK_RUC_VALIDO
        CHECK (ruc > 0)
);
GO

-- ============================================================================
-- PASO 6: CREAR ÍNDICES PARA OPTIMIZACIÓN (sin cambios del original)
-- ============================================================================
CREATE NONCLUSTERED INDEX IDX_CONTRIBUYENTE_UBICACION_ESTADO
    ON CONTRIBUYENTE(id_ubicacion, id_estado) INCLUDE (deuda);
GO

CREATE NONCLUSTERED INDEX IDX_CONTRIBUYENTE_DEUDA
    ON CONTRIBUYENTE(deuda DESC) WHERE deuda > 0;
GO

CREATE NONCLUSTERED INDEX IDX_CONTRIBUYENTE_CIIU
    ON CONTRIBUYENTE(id_ciiu) INCLUDE (deuda, id_estado);
GO

CREATE NONCLUSTERED INDEX IDX_UBICACION_NOMBRE
    ON UBICACION_GEOGRAFICA(nombre_distrito);
GO

-- ============================================================================
-- PASO 7: AGREGAR REGISTRO ESPECIAL (del script original)
-- ============================================================================
-- Agregar CIIU faltante si no existe
IF NOT EXISTS (SELECT 1 FROM ACTIVIDAD_ECONOMICA WHERE id_ciiu = '75113')
BEGIN
    INSERT INTO ACTIVIDAD_ECONOMICA (id_ciiu, descripcion, division)
    VALUES ('75113', 'Actividad económica código 75113', '75');
END
GO

-- ============================================================================
-- PASO 8: CREAR VISTA CON INFORMACIÓN COMPLETA (sin cambios del original)
-- ============================================================================
CREATE VIEW VW_CONTRIBUYENTE_COMPLETO AS
SELECT
    c.ruc,
    c.sexo,
    c.edad,
    c.deuda,
    ae.id_ciiu          AS codigo_ciiu,
    ae.descripcion      AS actividad_economica,
    te.descripcion      AS tipo_empresa,
    tm.descripcion      AS tamano_empresa,
    ub.nombre_distrito  AS distrito,
    ub.provincia,
    ub.departamento,
    et.descripcion      AS estado_tributario,
    cd.descripcion      AS condicion_domicilio
FROM CONTRIBUYENTE c
INNER JOIN ACTIVIDAD_ECONOMICA  ae ON c.id_ciiu          = ae.id_ciiu
INNER JOIN TIPO_EMPRESA         te ON c.id_tipo_empresa  = te.id_tipo_empresa
INNER JOIN TAMANO_EMPRESA       tm ON c.id_tamano        = tm.id_tamano
INNER JOIN UBICACION_GEOGRAFICA ub ON c.id_ubicacion     = ub.id_ubicacion
INNER JOIN ESTADO_TRIBUTARIO    et ON c.id_estado        = et.id_estado
INNER JOIN CONDICION_DOMICILIO  cd ON c.id_condicion     = cd.id_condicion;
GO

-- ============================================================================
-- PASO 9 (AGREGADO PARA DOCKER): TABLA PARA GUARDAR RESULTADOS DE SPARK
-- Esta tabla no existía en el script original.
-- Permite que los análisis de Spark persistan en la base de datos.
-- ============================================================================
CREATE TABLE RESULTADO_SPARK (
    id              INT IDENTITY(1,1) NOT NULL,
    nombre_analisis VARCHAR(200) NOT NULL,
    categoria       VARCHAR(200) NULL,
    metrica         VARCHAR(100) NOT NULL,
    valor_numerico  DECIMAL(18,4) NULL,
    valor_texto     VARCHAR(500)  NULL,
    fecha_ejecucion DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_RESULTADO_SPARK PRIMARY KEY (id)
);
GO

-- ============================================================================
-- VERIFICACIÓN: Mostrar tablas creadas
-- ============================================================================
SELECT
    t.name AS tabla,
    p.rows AS filas_aprox
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
ORDER BY t.name;
GO

PRINT 'Script SQL completado exitosamente - FAVISA_DB lista para recibir datos.';
GO
