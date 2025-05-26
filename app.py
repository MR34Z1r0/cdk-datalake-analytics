#!/usr/bin/env python3
import os
import aws_cdk as cdk
from cadena_analytics.cadena_analytics_stack import cadena_analytics_stack
from dotenv import load_dotenv
load_dotenv()
#load_dotenv('.env_desarrollo')
#load_dotenv('.env_productivo')

props = {
    'AWS_ACCOUNT' : os.getenv("AWS_ACCOUNT"),
    'REGION' : os.getenv("REGION"),
    'ENVIRONMENT' : os.getenv("ENVIRONMENT"),
    "ENTERPRISE" : "aje",
    'ARTIFACTS_BUCKET' : os.getenv("ARTIFACTS_BUCKET"),
    'STAGE_BUCKET' : os.getenv("STAGE_BUCKET"),
    'ANALYTICS_BUCKET' : os.getenv("ANALYTICS_BUCKET"),
    'LOGS_DYNAMO_TABLE' : os.getenv("LOGS_DYNAMO_TABLE"),
    'PROJECT_NAME' : os.getenv("PROJECT_NAME"),
    'ERROR_TOPIC_ARN' : os.getenv("ERROR_TOPIC_ARN"),
    'EXTERNAL_FILES_BUCKET' : os.getenv("EXTERNAL_FILES_BUCKET"),
    'CREDENTIALS_DYNAMO_TABLE' : os.getenv("CREDENTIALS_DYNAMO_TABLE"),
    'GLUE_CONN_REDSHIFT_JDBC' : os.getenv("GLUE_CONN_REDSHIFT_JDBC"),
    'GLUE_CONN_REDSHIFT_USER' : os.getenv("GLUE_CONN_REDSHIFT_USER"),
    'GLUE_CONN_REDSHIFT_PASS' : os.getenv("GLUE_CONN_REDSHIFT_PASS"),
    'GLUE_CONN_REDSHIFT_SG' : os.getenv("GLUE_CONN_REDSHIFT_SG"),
    'GLUE_CONN_REDSHIFT_SUBNET' : os.getenv("GLUE_CONN_REDSHIFT_SUBNET"),
    'GLUE_CONN_REDSHIFT_AVAILABILITY_ZONE' : os.getenv("GLUE_CONN_REDSHIFT_AVAILABILITY_ZONE"),
}

app = cdk.App()

analytics_stack = cadena_analytics_stack(
    app,
     f"Datalake-{props['ENVIRONMENT']}-analytics-Stack",
    env=cdk.Environment(account=props["AWS_ACCOUNT"], region=props["REGION"]),
    props = props
)

app.synth()
