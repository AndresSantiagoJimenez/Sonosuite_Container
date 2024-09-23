import os
import posixpath
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from loguru import logger
from config.settings import settings

def list_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files

def upload_missing_files_to_s3(directorio_local, bucket_name, s3_prefix_raw, s3_existing_files):
    s3_client = boto3.client('s3')
    
    # Definir los sufijos y prefijos S3
    extensions_prefixes = {
        '.zip': s3_prefix_raw
    }

    # Recorrer los archivos del directorio local
    for root, _, files in os.walk(directorio_local):
        for archivo in files:
            ext = os.path.splitext(archivo)[1]
            if ext in extensions_prefixes:
                # Obtener la ruta local del archivo
                archivo_local_path = os.path.join(root, archivo)

                # Obtener la ruta relativa respecto al directorio local
                relative_path = os.path.relpath(archivo_local_path, directorio_local).replace("\\", "/")

                # Definir la ruta en S3
                s3_path = posixpath.join(extensions_prefixes[ext], relative_path)

                # Comprobar si el archivo ya existe en S3
                if s3_path not in s3_existing_files:
                    try:
                        logger.info(f"Subiendo {archivo_local_path} a s3://{bucket_name}/{s3_path}")
                        # Subir el archivo a S3
                        s3_client.upload_file(archivo_local_path, bucket_name, s3_path)
                    except Exception as e:
                        logger.error(f"Error subiendo {archivo_local_path} a S3: {e}")
                else:
                    logger.info(f"El archivo {s3_path} ya existe en S3, omitiendo.")
