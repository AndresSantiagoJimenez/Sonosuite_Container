import boto3
import re

# Configura tu cliente S3
s3 = boto3.client('s3')

# Parámetros
BUCKET_NAME = 'sns-amazonmusic-trends'  # Reemplaza con el nombre de tu bucket
LOCAL_PATH_SALES = 'src/sales/'   # La ruta en el bucket

def delete_zip_files(bucket_name, prefix):
    # Lista los objetos en el bucket con el prefijo dado
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Verifica si la respuesta contiene objetos
    while response.get('Contents'):
        for obj in response['Contents']:
            file_key = obj['Key']
            # Verifica si el archivo tiene la extensión .zip
            if file_key.endswith('.zip'):
                print(f'Eliminando {file_key}...')
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                print(f'Archivo {file_key} eliminado.')
        
        # Obtiene el marcador para la siguiente página de resultados, si lo hay
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            break

# Llama a la función para eliminar los archivos .zip
delete_zip_files(BUCKET_NAME, LOCAL_PATH_SALES)