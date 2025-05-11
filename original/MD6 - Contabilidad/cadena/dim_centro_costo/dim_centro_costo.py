import boto3
import datetime as dt
import logging
import os
import pytz
import sys
from aje.get_schemas import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    when,
)

######################################
# JOB PARAMETERS

args = getResolvedOptions(
    sys.argv,
    [
        "S3_PATH_STG_BM",
        "S3_PATH_STG_SF",
        "S3_PATH_DOM",
        "S3_PATH_COM",
        "REGION_NAME",
        "DYNAMODB_DATABASE_NAME",
        "COD_PAIS",
        "DYNAMODB_LOGS_TABLE",
        "ERROR_TOPIC_ARN",
        "PROJECT_NAME",
        "FLOW_NAME",
        "PROCESS_NAME",
        "S3_PATH_CAD",
        "S3_PATH_EXTERNAL_DATA",
    ],
)

S3_PATH_STG_BM = args["S3_PATH_STG_BM"]
S3_PATH_STG_SF = args["S3_PATH_STG_SF"]
S3_PATH_DOM = args["S3_PATH_DOM"]
S3_PATH_COM = args["S3_PATH_COM"]
S3_PATH_CAD = args["S3_PATH_CAD"]
S3_PATH_EXTERNAL_DATA = args["S3_PATH_EXTERNAL_DATA"]
REGION_NAME = args["REGION_NAME"]
DYNAMODB_DATABASE_NAME = args["DYNAMODB_DATABASE_NAME"]
COD_PAIS = args["COD_PAIS"]

DYNAMODB_LOGS_TABLE = args["DYNAMODB_LOGS_TABLE"]
ERROR_TOPIC_ARN = args["ERROR_TOPIC_ARN"]
PROJECT_NAME = args["PROJECT_NAME"]
FLOW_NAME = args["FLOW_NAME"]
PROCESS_NAME = args["PROCESS_NAME"]

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(PROCESS_NAME)
logger.setLevel(os.environ.get("LOGGING", logging.DEBUG))

TZ_LIMA = pytz.timezone("America/Lima")
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

logger.info(f"project name: {PROJECT_NAME} | flow name:  {FLOW_NAME} | process name: {PROCESS_NAME}")
logger.info(f"COD_PAIS: {COD_PAIS}")

######################################
# JOB SETUP

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)

sc = spark.sparkContext
glue_context = GlueContext(sc)
# logger = glue_context.get_logger()
# sqlContext = SQLContext(sparkSession=spark, sparkContext=sc)
dynamodb_client = boto3.client("dynamodb", region_name=REGION_NAME)
sns_client = boto3.client("sns", region_name=REGION_NAME)

######################################
# FUNCTIONS
def add_log_to_dynamodb(task_name, error_message=""):
    dynamodb_client = boto3.resource("dynamodb")
    dynamo_table = dynamodb_client.Table(DYNAMODB_LOGS_TABLE)
    task_status = "satisfactorio" if error_message == "" else "error"
    process_type = "F"
    date_system = NOW_LIMA.strftime("%Y%m%d_%H%M%S")
    print(f"Adding log to DynamoDB, PROCESS_ID: DLB_{PROCESS_NAME}_{date_system}")

    record = {
        "PROCESS_ID": f"DLB_{PROCESS_NAME}_{date_system}",
        "DATE_SYSTEM": date_system,
        "PROJECT_NAME": args["PROJECT_NAME"],
        "FLOW_NAME": args["FLOW_NAME"],
        "TASK_NAME": task_name,
        "TASK_STATUS": task_status,
        "MESSAGE": str(error_message),
        "PROCESS_TYPE": process_type,
    }
    dynamo_table.put_item(Item=record)

def send_error_message(table_name, msg, error):
    response = sns_client.publish(
        TopicArn=ERROR_TOPIC_ARN,
        Message=f"Failed table: {table_name}\nMessage: {msg}\nLog ERROR:\n{error}"
    )

def read_table(table_name, path):
    try:
        s3_path = f"{path}/{table_name}"
        df = spark.read.format("delta").load(s3_path)
        # logger.info(f"Tabla {table_name} leida correctamente. Registros leidos: {df.count()}")
        try:
            df = df.filter(col("id_pais").isin(COD_PAIS))
            # logger.info(f"Registros filtrados por COD_PAIS: {df.count()}")
        except Exception as e:
            logger.info(f"Columna id_pais no encontrada en tabla {table_name}")
        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Leyendo tablas", e)
        # send_error_message(PROCESS_NAME, "Leyendo tablas", f"{str(e)[:10000]}")
        exit(1)

def create_df_schema(table_name):
    try:
        schemas = SchemaModeloComercial(logger)
        schema = schemas.get_schema(table_name)
        df = spark.createDataFrame([], schema)
        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Creando schema", e)
        # send_error_message(PROCESS_NAME, "Creando schema", f"{str(e)[:10000]}")
        exit(1)

def get_table(table_name, path):
    s3_path = f"{path}/{table_name}"
    try:
        df = spark.read.format("delta").load(s3_path)
        # logger.info(f"Table {table_name}_com loaded. Count: {df.count()}")
        try:
            df = df.filter(col("id_pais").isin(COD_PAIS))
            # logger.info(f"Registros filtrados por COD_PAIS: {df.count()}")
        except Exception as e:
            logger.info(f"Columna id_pais no encontrada en tabla {table_name}")
    except AnalysisException:
        # If Path does not exist:
        logger.info(f"Path does not exist: {s3_path}. Proceeding to create schema")
        df = create_df_schema(table_name)
        logger.info(f"Schema created for table {table_name}:")
        df.printSchema()
    return df

######################################
# READ
try:
    # Load Dominio
    df_m_medio_transporte = read_table("m_medio_transporte", S3_PATH_DOM)
    
    #Load Cadena
    df_dim_medio_transporte_com = get_table("dim_medio_transporte", S3_PATH_COM)
    logger.info("Dataframes cargados correctamente")

except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb("Leyendo tablas fuente", e)
    # send_error_message(PROCESS_NAME, "Leyendo tablas fuente", f"{str(e)[:10000]}")
    exit(1)

else:
    # CREATE
    try:
        logger.info("Starting creation of tmp_cadena_dim_medio_transporte")
        tmp_cadena_dim_medio_transporte = (
            df_m_medio_transporte
            # .where(col("id_pais").isin(COD_PAIS))
            .select(
                col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
                col("id_pais").cast("string").alias("id_pais"),
                col("cod_medio_transporte").cast("string").alias("cod_medio_transporte"),
                col("cod_tipo_medio_transporte").cast("string").alias("cod_tipo_medio_transporte"),
                col("desc_tipo_medio_transporte").cast("string").alias("desc_tipo_medio_transporte"),
                col("desc_marca_medio_transporte").cast("string").alias("desc_marca_medio_transporte"),
                col("cod_tipo_capacidad").cast("string").alias("cod_tipo_capacidad"),
                col("cant_peso_maximo").cast("integer").alias("cant_peso_maximo"),
                col("cant_tarimas").cast("string").alias("cant_tarimas"),
                col("fecha_creacion").cast("date").alias("fecha_creacion"),
                col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
            )
        )
        # logger.info(f"tmp_cadena_dim_medio_transporte count: {tmp_cadena_dim_medio_transporte.count()}")    

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Creando tablas", e)
        # send_error_message(PROCESS_NAME, "Creando tablas", f"{str(e)[:10000]}")
        exit(1)

    # INSERT
    try:
        conditions = (
            tmp_cadena_dim_medio_transporte["id_medio_transporte"] == df_dim_medio_transporte_com["id_medio_transporte"]
        ) & (tmp_cadena_dim_medio_transporte["id_pais"] == df_dim_medio_transporte_com["id_pais"])

        different_df = tmp_cadena_dim_medio_transporte.join(df_dim_medio_transporte_com, conditions, "left_anti")
        # logger.info(f"different_df count after left_anti join: {different_df.count()}")

        df_dim_medio_transporte_com = df_dim_medio_transporte_com.unionByName(different_df)
        # logger.info(f"df_dim_medio_transporte_com count after union: {df_dim_medio_transporte_com.count()}")

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Insertando datos", e)
        # send_error_message(PROCESS_NAME, "Insertando datos", f"{str(e)[:10000]}")
        exit(1)

    # UPDATE
    try:

        logger.info("Starting UPDATE operation...")

        df_dim_medio_transporte_com = (
            df_dim_medio_transporte_com.alias("a")
            .join(
                tmp_cadena_dim_medio_transporte.alias("b"),
                (col("a.id_medio_transporte") == col("b.id_medio_transporte"))
                & (col("a.id_pais") == col("b.id_pais")),
                "inner",
            )
            .select(
                col("a.id_medio_transporte").alias("id_medio_transporte"),
                coalesce(col("b.id_pais"), col("a.id_pais")).alias("id_pais"),
                coalesce(col("b.cod_medio_transporte"), col("a.cod_medio_transporte")).alias("cod_medio_transporte"),
                coalesce(col("b.cod_tipo_medio_transporte"), col("a.cod_tipo_medio_transporte")).alias("cod_tipo_medio_transporte"),
                coalesce(col("b.desc_tipo_medio_transporte"), col("a.desc_tipo_medio_transporte")).alias("desc_tipo_medio_transporte"),
                coalesce(col("b.desc_marca_medio_transporte"), col("a.desc_marca_medio_transporte")).alias("desc_marca_medio_transporte"),
                coalesce(col("b.cod_tipo_capacidad"), col("a.cod_tipo_capacidad")).alias("cod_tipo_capacidad"),
                coalesce(col("b.cant_peso_maximo").cast("integer"), col("a.cant_peso_maximo").cast("integer")).alias("cant_peso_maximo"),
                coalesce(col("b.cant_tarimas"), col("a.cant_tarimas")).alias("cant_tarimas"),
                coalesce(col("b.fecha_creacion"), col("a.fecha_creacion")).alias("fecha_creacion"),
                coalesce(col("b.fecha_modificacion"), col("a.fecha_modificacion")).alias("fecha_modificacion"),
            )
        )
        # logger.info(f"df_dim_medio_transporte_com count after UPDATE operation: {df_dim_medio_transporte_com.count()}")

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Actualizando tablas", e)
        # send_error_message(PROCESS_NAME, "Actualizando tablas", f"{str(e)[:10000]}")
        exit(1)
    # SAVE
    try:
        logger.info("Starting SAVE operation...")
        table_name = "dim_medio_transporte"
        df = df_dim_medio_transporte_com
        s3_path_com = f"{S3_PATH_COM}/{table_name}"
        logger.info(f"Guardando tabla: {table_name} en path: {s3_path_com}")

        partition_columns_array = ["id_pais"]

        df.write.partitionBy(*partition_columns_array) \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("partitionOverwriteMode", "dynamic") \
            .save(s3_path_com)

        delta_table = DeltaTable.forPath(spark, s3_path_com)
        delta_table.vacuum(retentionHours=168)
        delta_table.generate("symlink_format_manifest")
        logger.info(f"Tabla guardada correctamente: {table_name} con {df.count()} registros. Esquema:")
        df.printSchema()

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Guardando tablas", e)
        # send_error_message(PROCESS_NAME, "Guardando tablas", f"{str(e)[:10000]}")
        exit(1)
