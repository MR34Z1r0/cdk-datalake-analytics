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
)

######################################
# JOB PARAMETERS

args = getResolvedOptions(
    sys.argv,
    [
        "S3_PATH_STG_BM",
        "S3_PATH_STG_SF",
        "S3_PATH_DOM",
        "REGION_NAME",
        "DYNAMODB_DATABASE_NAME",
        "COD_PAIS",
        "DYNAMODB_LOGS_TABLE",
        "ERROR_TOPIC_ARN",
        "PROJECT_NAME",
        "FLOW_NAME",
        "PROCESS_NAME",
    ],
)

S3_PATH_STG_BM = args["S3_PATH_STG_BM"]
S3_PATH_STG_SF = args["S3_PATH_STG_SF"]
S3_PATH_DOM = args["S3_PATH_DOM"]
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

def get_databases_from_dynamodb(COD_PAIS):
    response = dynamodb_client.scan(
        TableName=DYNAMODB_DATABASE_NAME,
        AttributesToGet=["ENDPOINT_NAME"],
    )
    endpoint_names = [
        item["ENDPOINT_NAME"]["S"]
        for item in response["Items"]
        if item["ENDPOINT_NAME"]["S"].startswith(COD_PAIS)
    ]
    return endpoint_names

def create_df_union(table_name, databases, s3_path):
    df = None
    for bd in databases:
        try:
            if df is None:
                df = read_table_stg(f"{s3_path}/{bd}", table_name)
                if df.count() == 0:
                    continue
            else:
                df_bd = read_table_stg(f"{s3_path}/{bd}", table_name)
                if df_bd.count() == 0:
                    continue
                df = df.union(df_bd)
        except AnalysisException as e:
            logger.error(str(e))
            add_log_to_dynamodb("Leyendo tablas ingesta", e)
            continue

    if df is None:
        add_log_to_dynamodb("Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
        # send_error_message(PROCESS_NAME, "Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
        exit(1)
    else:
        logger.info(f"Table {table_name} loaded. Count: {df.count()}")
        return df


def read_table_stg(s3_path, table_name):
    s3_path = f"{s3_path}/{table_name}/"
    df = spark.read.option("basePath", s3_path).format("delta").load(s3_path)
    return df

def read_table_stg_salesforce(table_name):
    try:
        s3_path = f"{S3_PATH_STG_SF}/{table_name}/"
        df = spark.read.format("delta").load(s3_path)
        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Leyendo tablas salesforce", e)
        # send_error_message(PROCESS_NAME, "Leyendo tablas salesforce", f"{str(e)[:10000]}")
        exit(1)

def create_df_schema(table_name):
    try:
        schemas = SchemaDominioCadena(logger)
        schema = schemas.get_schema(table_name)
        df = spark.createDataFrame([], schema)
        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Creando schema", e)
        # send_error_message(PROCESS_NAME, "Creando schema", f"{str(e)[:10000]}")
        exit(1)

def read_table(table_name, path):
    s3_path = f"{path}/{table_name}"
    try:
        df = spark.read.format("delta").load(s3_path)
        logger.info(f"Tabla {table_name}_dom leida correctamente. Registros leidos: {df.count()}")
        try:
            df = df.filter(col("id_pais").isin(COD_PAIS))
            logger.info(f"Registros filtrados por COD_PAIS: {df.count()}")
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
    # databases = get_databases_from_dynamodb(COD_PAIS)
    databases = ["PEBDAJEP1QA"]
    logger.info(f"Databases: {databases}")
    # Load Stage
    df_m_compania = create_df_union("m_compania", databases, S3_PATH_STG_BM)
    df_m_pais = create_df_union("m_pais", databases, S3_PATH_STG_BM)
    df_m_centro_costo = create_df_union("m_centro_costo", databases, S3_PATH_STG_BM)
    # Load Dominio
    df_m_centro_costo_dom = read_table("m_centro_costo", S3_PATH_DOM)

    logger.info("Dataframes cargados correctamente")

except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb("Leyendo tablas fuente", e)
    # send_error_message(PROCESS_NAME, "Leyendo tablas fuente", f"{str(e)[:10000]}")
    exit(1)

else:
    # CREATE
    try:
        logger.info("Starting creation of tmp_dominio_m_centro_costo")
        tmp_dominio_m_centro_costo = (
            df_m_centro_costo.alias("mcc")
            .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("mcc.cod_compania"), "inner")
            .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
            .filter(col("mp.id_pais").isin(COD_PAIS))
            .select(
                col("mp.id_pais"),
                col("mcc.id_compania"),
                col("mcc.id_centro_costo"),
                col("mcc.id_centro_costo_corp"),
                col("mcc.id_area"),
                col("mcc.id_gerencia"),
                col("mcc.cod_ejercicio"),
                col("mcc.cod_centro_costo"),
                col("mcc.desc_centro_costo"),
                col("mcc.cod_centro_costo_corp"),
                # col("mcc.desc_centro_costo_corp"), # no encuentro este campo en la ingesta
                lit(None).alias("desc_centro_costo_corp"), # delete this line when the field is available
                col("mcc.cod_area"),
                col("mcc.cod_gerencia"),
                col("mcc.cod_tipo"),
                col("mcc.cod_tipo_almacen"),
                col("mcc.estado"),
                col("mcc.fecha_creacion"),
                col("mcc.fecha_modificacion")
            )
        )
        logger.info(f"tmp_dominio_m_centro_costo count: {tmp_dominio_m_centro_costo.count()}")

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Creando tablas", e)
        # send_error_message(PROCESS_NAME, "Creando tablas", f"{str(e)[:10000]}")
        exit(1)

    # INSERT
    try:
        logger.info("Starting INSERT operation...")
        conditions = (
            tmp_dominio_m_centro_costo["id_centro_costo"]
            == df_m_centro_costo_dom["id_centro_costo"]
        ) & (
            tmp_dominio_m_centro_costo["id_pais"]
            == df_m_centro_costo_dom["id_pais"]
        )

        different_df = tmp_dominio_m_centro_costo.join(df_m_centro_costo_dom, conditions, "left_anti")
        # logger.info(f"different_df count after left_anti join: {different_df.count()}")

        df_m_centro_costo_dom = df_m_centro_costo_dom.unionByName(different_df)
        # logger.info(f"df_m_centro_costo_dom count after union: {df_m_centro_costo_dom.count()}")

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Insertando datos", e)
        # send_error_message(PROCESS_NAME, "Insertando datos", f"{str(e)[:10000]}")
        exit(1)

    # UPDATE
    try:

        logger.info("Starting UPDATE operation...")

        df_m_centro_costo_dom = (
            df_m_centro_costo_dom.alias("a")
            .join(
                tmp_dominio_m_centro_costo.alias("b"),
                (col("a.id_centro_costo") == col("b.id_centro_costo"))
                & (col("a.id_pais") == col("b.id_pais")),
                "inner",
            )
            .select(
                col("a.id_centro_costo").alias("id_centro_costo"),
                coalesce(col("b.id_pais"), col("a.id_pais")).alias("id_pais"),
                coalesce(col("b.id_compania"), col("a.id_compania")).alias("id_compania"),
                coalesce(col("b.id_centro_costo_corp"), col("a.id_centro_costo_corp")).alias("id_centro_costo_corp"),
                coalesce(col("b.id_area"), col("a.id_area")).alias("id_area"),
                coalesce(col("b.id_gerencia"), col("a.id_gerencia")).alias("id_gerencia"),
                coalesce(col("b.cod_ejercicio"), col("a.cod_ejercicio")).alias("cod_ejercicio"),
                coalesce(col("b.cod_centro_costo"), col("a.cod_centro_costo")).alias("cod_centro_costo"),
                coalesce(col("b.desc_centro_costo"), col("a.desc_centro_costo")).alias("desc_centro_costo"),
                coalesce(col("b.cod_centro_costo_corp"), col("a.cod_centro_costo_corp")).alias("cod_centro_costo_corp"),
                coalesce(col("b.desc_centro_costo_corp"), col("a.desc_centro_costo_corp")).alias("desc_centro_costo_corp"),
                coalesce(col("b.cod_area"), col("a.cod_area")).alias("cod_area"),
                coalesce(col("b.cod_gerencia"), col("a.cod_gerencia")).alias("cod_gerencia"),
                coalesce(col("b.cod_tipo"), col("a.cod_tipo")).alias("cod_tipo"),
                coalesce(col("b.cod_tipo_almacen"), col("a.cod_tipo_almacen")).alias("cod_tipo_almacen"),
                coalesce(col("b.estado"), col("a.estado")).alias("estado"),
                coalesce(col("b.fecha_creacion"), col("a.fecha_creacion")).alias("fecha_creacion"),
                coalesce(col("b.fecha_modificacion"), col("a.fecha_modificacion")).alias("fecha_modificacion"),
            )
        )
        logger.info(f"df_m_centro_costo_dom count after UPDATE operation: {df_m_centro_costo_dom.count()}")

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Actualizando tablas", e)
        # send_error_message(PROCESS_NAME, "Actualizando tablas", f"{str(e)[:10000]}")
        exit(1)

    # SAVE
    try:
        logger.info("Starting SAVE operation...")
        table_name = "m_centro_costo"
        df = df_m_centro_costo_dom
        s3_path_dom = f"{S3_PATH_DOM}/{table_name}"
        logger.info(f"Guardando tabla: {table_name} en path: {s3_path_dom}")

        partition_columns_array = ["id_pais"]

        df.write.partitionBy(*partition_columns_array) \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("partitionOverwriteMode", "dynamic") \
            .save(s3_path_dom)

        delta_table = DeltaTable.forPath(spark, s3_path_dom)
        delta_table.vacuum(retentionHours=168)
        delta_table.generate("symlink_format_manifest")
        logger.info(f"Tabla guardada correctamente: {table_name} con {df.count()} registros. Esquema:")
        df.printSchema()

    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Guardando tablas", e)
        # send_error_message(PROCESS_NAME, "Guardando tablas", f"{str(e)[:10000]}")
        exit(1)
