"""
Recorre los archivos .zip dentro de la estructura de 'src/sales/' en S3 y los mueve a 'src/raw/', 
manteniendo la estructura de carpetas de países y servicios.

:param bucket_name: El nombre del bucket en S3.
:param s3_prefix_sales: Prefijo en S3 de la carpeta 'src/sales/'.
:param s3_prefix_raw: Prefijo en S3 de la carpeta 'src/raw/' donde se moverán los archivos.
"""

import boto3
import posixpath
from loguru import logger
from config.settings import settings

def mover_archivos_zip_a_raw(bucket_name, s3_prefix_sales, s3_prefix_raw):
    # Crear cliente S3
    s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    # Paginador para listar todos los archivos bajo el prefijo 'src/sales/'
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        # Recorrer las páginas de resultados
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix_sales):
            for obj in page.get('Contents', []):
                archivo_s3 = obj['Key']
                
                # Verificar si el archivo es un .zip
                if archivo_s3.endswith('.zip'):
                    logger.info(f"Archivo .zip encontrado: {archivo_s3}")
                    
                    # Obtener la estructura relativa bajo 'src/sales/'
                    relative_path = archivo_s3[len(s3_prefix_sales):]
                    
                    # Crear el nuevo path bajo 'src/raw/' manteniendo la estructura
                    nuevo_s3_path = posixpath.join(s3_prefix_raw, relative_path)
                    
                    logger.info(f"Moviendo archivo a: s3://{bucket_name}/{nuevo_s3_path}")
                    
                    # Copiar el archivo a la nueva ubicación en 'src/raw/'
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': archivo_s3},
                        Key=nuevo_s3_path
                    )
                    
                    # Eliminar el archivo original de 'src/sales/' tras la copia exitosa
                    s3.delete_object(Bucket=bucket_name, Key=archivo_s3)
                    logger.info(f"Archivo original {archivo_s3} eliminado de 'src/sales/'.")
    
    except Exception as e:
        logger.error(f"Error al mover archivos .zip a 'src/raw/': {e}")

# Parámetros
bucket_name = 'sns-amazonmusic-trends'
s3_prefix_sales = 'src/sales/'
s3_prefix_raw = 'src/raw/'

# Llamada a la función
mover_archivos_zip_a_raw(bucket_name, s3_prefix_sales, s3_prefix_raw)
