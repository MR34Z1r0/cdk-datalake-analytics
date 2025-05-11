import boto3
import csv

# Configura tu cliente de DynamoDB
session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    aws_session_token='',
    region_name='us-east-1'
)

dynamodb = session.resource('dynamodb', region_name='us-east-1')  # Cambia a tu región
table_name = 'dev-configuration-dynamo-table'
table = dynamodb.Table(table_name)

def subir_csv_a_dynamo(archivo_csv):
    with open(archivo_csv, 'r') as archivo:
        reader = csv.DictReader(archivo)  # Lee el CSV como diccionario
        for fila in reader:
            try:
                # Inserta cada fila en la tabla
                fila['ACTIVE_FLAG'] = True

                response = table.put_item(Item=fila)
                print(f"Elemento subido: {fila}")
            except Exception as e:
                print(f"Error subiendo {fila}: {e}")


subir_csv_a_dynamo('./config_table_dms.csv')



