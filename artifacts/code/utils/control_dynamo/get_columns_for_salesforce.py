import boto3
import csv

# Configura tu cliente de DynamoDB
session = boto3.Session( region_name='us-east-1')

client = boto3.client('appflow')

def get_flow_columns(flow_name):
    response = client.describe_flow(flowName=flow_name)
    try:
        name = []
        for task in response['tasks']:
            for source_fields in task['sourceFields']:
                name.append(source_fields)
        name = list(set(name))
        return ','.join(name)

    except Exception as e:
        return ""
        return columns_string

flow_name = [
    "dev-unacemec-appflow-salesforce-investigacionpreciosc-full",
    'dev-unacemec-appflow-salesforce-investigacingeneraldemercadoc-full',
    'dev-unacemec-appflow-salesforce-lead-full'
]

for f in flow_name:
    try:
        columns = get_flow_columns(f)
        print(f"Columnas del flujo '{f}':\n{columns}")
        print("\n")
    except Exception as e:
        print(e)