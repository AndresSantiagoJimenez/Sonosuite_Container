import posixpath
import pandas as pd
import zipfile
import os
import boto3
from loguru import logger
from config.settings import settings
from io import StringIO


# Inicializar el cliente de S3
s3_client = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

def transformar_datos(df):
    """
    Transforma los datos en el DataFrame, renombrando columnas a un formato estandarizado.
    :param df: DataFrame a transformar.
    :return: DataFrame transformado, o None en caso de error.
    """
    try:
        df = df.rename(columns={
            'dataset date': 'Dataset_date',
            'territory code': 'Territory_code',
            'track asin': 'Track_asin',
            'track isrc': 'Track_isrc',
            'proprietery track id': 'Proprietery_track_id',
            'track name': 'Track_name',
            'track artist': 'Track_artist',
            'album asin': 'Album_asim',
            'digital album upc': 'Digital_album_upc',
            'album name': 'Album_name',
            'album artist': 'Album_artist',
            'offline plays': 'Offline_plays',
            'streams': 'Streams',
            'timestamp': 'Timestamp',
            'play duration': 'Play_duration',
            'subscription plan': 'Subscription_plan',
            'device type': 'Device_type',
            'customer id': 'Customer_id',
            'postal code': 'Postal_code',
            'stream source': 'Stream_source',
            'stream source id': 'Stream_source_id',
            'stream source name': 'Stream_source_name',
            'track quality': 'Track_quality',
            'asset type': 'Asset_type'
        })
        df = df.rename(columns=lambda x: x.upper())
        return df
    except Exception as e:
        logger.error(f"Error al transformar los datos: {e}")
        return None

def guardar_json_s3(df, bucket, ruta_s3):
    """
    Guarda el DataFrame en formato JSON directamente en un bucket de S3.
    :param df: DataFrame a guardar.
    :param bucket: Nombre del bucket de S3.
    :param ruta_s3: Ruta del archivo JSON en el bucket de S3.
    """
    try:
        # Convertir el DataFrame a JSON en formato de líneas
        json_data = df.to_json(orient='records', lines=True)
        
        # Convertir el JSON a un stream para subirlo a S3
        json_buffer = StringIO(json_data)
        
        # Subir el archivo a S3
        s3_client.put_object(Body=json_buffer.getvalue(), Bucket=bucket, Key=ruta_s3)
        
        #logger.info(f"Archivo JSON guardado en S3: {ruta_s3}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo JSON en S3: {e}")
        
        
def upload_and_transform_txt_files_to_s3(archivo_txt, bucket, s3_prefix_raw, ruta_local_base):
    try:
        # Leer el archivo TXT
        df = pd.read_csv(archivo_txt, delimiter='\t')

        # Transformar los datos
        df_transformado = transformar_datos(df)
        if df_transformado is None:
            logger.error(f"Error al transformar los datos para el archivo: {archivo_txt}")
            return

        # Obtener la ruta relativa del archivo respecto a la carpeta local base
        ruta_relativa = os.path.relpath(archivo_txt, ruta_local_base).replace("\\", "/")

        # Definir la ruta del archivo JSON en S3 manteniendo la estructura de carpetas
        nombre_archivo_json = os.path.basename(archivo_txt).replace('.txt', '.json')
        ruta_s3 = posixpath.join(s3_prefix_raw, os.path.dirname(ruta_relativa), nombre_archivo_json)

        # Subir el DataFrame transformado a S3 como JSON
        s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        with open("/tmp/temp.json", 'w') as f:
            df_transformado.to_json(f, orient='records')

        logger.info(f"Subiendo {archivo_txt} como JSON a s3://{bucket}/{ruta_s3}")
        s3.upload_file('/tmp/temp.json', bucket, ruta_s3)

        # Eliminar el archivo TXT original si todo fue exitoso
        os.remove(archivo_txt)
        logger.info(f"Archivo TXT original eliminado: {archivo_txt}")

    except Exception as e:
        logger.error(f"Error procesando el archivo {archivo_txt}: {e}")

