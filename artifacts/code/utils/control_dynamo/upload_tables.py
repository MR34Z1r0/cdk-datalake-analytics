import boto3
import csv

# Configura tu cliente de DynamoDB
session = boto3.Session(
    profile_name="unacem-qa",
    region_name='us-east-1'
)

dynamodb = session.resource('dynamodb', region_name='us-east-1')  # Cambia a tu región
table_name = 'qa-data-platform-configuration-dynamo-table'
table = dynamodb.Table(table_name)

# Sube un archivo CSV a la tabla de DynamoDB

def subir_csv_a_dynamo(archivo_csv):
    with open(archivo_csv, 'r') as archivo:
        reader = csv.DictReader(archivo, delimiter=";")  # Lee el CSV como diccionario
        for fila in reader:
            try:
                # Inserta cada fila en la tabla
                fila['ACTIVE_FLAG'] = True
                fila['PROCESS_ID'] = "10"
                print(f"fila: {fila}")
                response = table.put_item(Item=fila)
                print(f"Elemento subido: {fila}")
            except Exception as e:
                print(f"Error subiendo {fila}: {e}")
                break


subir_csv_a_dynamo('./results.csv')



