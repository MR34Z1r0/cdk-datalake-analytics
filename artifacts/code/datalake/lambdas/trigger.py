import boto3
import os


def lambda_handler(event, context):
    try:
        COD_PROCESO = event["COD_PROCESO"]
        REDSHIFT_SOURCE_LIST = os.environ["REDSHIFT_SOURCE_LIST"].split(",")

        dynamo = boto3.resource("dynamodb")
        table = dynamo.Table(os.environ["CONFIG_PROCESOS"])
        tables = table.query(
            IndexName="PROCESS_ID-index",
            KeyConditionExpression=Key("PROCESS_ID").eq(COD_PROCESO),
        )

        if not tables["Items"]:
            return {
                "error": True,
                "msg": f"COD_PROCESO {COD_PROCESO} no existe",
            }

        table_list = []
        redshift_list = []
        for table in tables["Items"]:
            table_list.append(table["TABLE"])
            if table["SOURCE"] in REDSHIFT_SOURCE_LIST:
                redshift_list.append({"table": table["TABLE"], "db_table": table["DBTABLE_NAME"], "s3_path": table["S3_PATH"]})

        int_prev_periods = -2
        
        if "PERIODOS" in event:
            periodos = event["PERIODOS"]
        else:
            periodos = "-"

        return {
            "error": False,
            "COD_PAIS": event["COD_PAIS"],
            "PERIODO_INI": event["PERIODO_INI"],
            "PERIODO_FIN": event["PERIODO_FIN"],
            "PERIODOS": periodos,
            "tableList": table_list,
            "REDSHIFT_LIST": redshift_list,
        }
    except KeyError as e:
        return {
            "error": True,
            "msg": f"Parámetro no encontrado {str(e)}",
        }
