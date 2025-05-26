import sys
import boto3
from pyspark.sql.functions import col, coalesce, lit, substring, lpad, cast
from decimal import Decimal
import datetime as dt
import pytz
import logging
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths

spark_controller = SPARK_CONTROLLER()

try:

    df_centro_costo = spark_controller.read_table(data_paths.DOMINIO, "m_centro_costo")
    target_table_name = "dim_centro_costo"
    
except Exception as e:
    logger.error(e)
    raise

try:
   
    df_centro_costo.show()
    

except Exception as e:
    logger.error(e)
    raise