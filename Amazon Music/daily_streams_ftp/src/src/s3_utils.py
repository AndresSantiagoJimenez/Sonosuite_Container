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

def upload_missing_files_to_s3(directorio_local, bucket_name, s3_prefix_raw, s3_prefix_sales, s3_existing_files):
    s3_client = boto3.client('s3')
    for root, dirs, files in os.walk(directorio_local):
        for archivo in files:
            if archivo.endswith('.json'):
                archivo_local_path = os.path.join(root, archivo)
                relative_path = os.path.relpath(archivo_local_path, directorio_local)
                s3_path = posixpath.join(s3_prefix_sales, relative_path.replace("\\", "/"))
            elif archivo.endswith('.zip'):
                archivo_local_path = os.path.join(root, archivo)
                relative_path = os.path.relpath(archivo_local_path, directorio_local)
                s3_path = posixpath.join(s3_prefix_raw, relative_path.replace("\\", "/"))
            else:
                continue  # Si el archivo no es .json ni .zip, lo ignoramos

            if s3_path not in s3_existing_files:
                try:
                    logger.info(f"Subiendo {archivo_local_path} a s3://{bucket_name}/{s3_path}")
                    s3_client.upload_file(archivo_local_path, bucket_name, s3_path)
                except Exception as e:
                    logger.error(f"Error subiendo {archivo_local_path} a S3: {e}")
            else:
                logger.info(f"El archivo {s3_path} ya existe en S3, omitiendo.")
                