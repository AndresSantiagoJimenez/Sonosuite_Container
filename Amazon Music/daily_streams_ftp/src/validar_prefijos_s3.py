import boto3
import os
from loguru import logger
import pandas as pd
from io import StringIO
from config.settings import settings
import zipfile
import tempfile
import shutil

# Configuración de los prefijos y el bucket
bucket_name = 'sns-amazonmusic-trends'
S3_PREFIX_RAW = 'src/raw/'  # Prefijo en S3 para raw (archivos .zip)
LOCAL_PATH_SALES = 'src/sales/'  # Prefijo en S3 para sales (archivos .json)
TEMPORAL_PATH = 'src/Data/'  # Carpeta temporal local para archivos descomprimidos

# Cliente de S3
s3_client = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)


# Asegurarse de que la carpeta temporal exista
if not os.path.exists(TEMPORAL_PATH):
    os.makedirs(TEMPORAL_PATH)

def get_s3_files(prefix):
    """Obtiene una lista de archivos en S3 bajo el prefijo dado."""
    s3_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3_files.append(obj['Key'])
    return s3_files

def get_local_files(directory):
    """Obtiene una lista de archivos en el directorio local dado."""
    local_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            local_files.append(os.path.relpath(os.path.join(root, file), directory))
    return local_files

def download_files_from_s3(missing_files):
    """Descarga los archivos que faltan en local desde S3 a la carpeta temporal."""
    for file_key in missing_files:
        file_name = file_key.split('/')[-1]  # Obtener solo el nombre del archivo
        local_file_path = os.path.join(TEMPORAL_PATH, file_name)
        print(f"Descargando {file_key} a {local_file_path}...")
        s3_client.download_file(bucket_name, file_key, local_file_path)
        print(f"Archivo {file_name} descargado.")

def compare_and_download_files(s3_files, local_files):
    """Compara los archivos en S3 (raw) y en local (sales), descarga los que faltan."""
    # Normalizar los nombres de archivos, eliminando extensiones y prefijos
    s3_files_normalized = {file.replace(S3_PREFIX_RAW, '').replace('.txt.zip', '').replace('.zip', '') for file in s3_files}
    local_files_normalized = {file.replace('.json', '') for file in local_files}

    # Encontrar archivos que están en S3 pero no en la carpeta local
    missing_in_local = s3_files_normalized - local_files_normalized

    if missing_in_local:
        print(f'Archivos en raw pero faltan en sales: {missing_in_local}')
        # Obtener la lista completa de rutas de archivos en S3 que faltan en local
        missing_s3_files = [file for file in s3_files if file.replace(S3_PREFIX_RAW, '').replace('.txt.zip', '').replace('.zip', '') in missing_in_local]
        # Descargar los archivos que faltan
        download_files_from_s3(missing_s3_files)
    else:
        print('Todos los archivos de raw tienen su contraparte en sales.')

def main():
    # Obtener archivos en S3 (raw) y archivos locales (sales)
    s3_files = get_s3_files(S3_PREFIX_RAW)
    local_files = get_local_files(LOCAL_PATH_SALES)
    
    # Comparar los archivos y descargar los que faltan
    compare_and_download_files(s3_files, local_files)

if __name__ == '__main__':
    main()