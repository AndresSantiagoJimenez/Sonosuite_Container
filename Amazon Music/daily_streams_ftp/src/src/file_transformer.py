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

def procesar_y_guardar_en_s3(archivo_txt, bucket, ruta_s3_base, ruta_local_base):
    """
    Procesa un archivo TXT delimitado por tabulaciones, lo transforma y lo sube como JSON a S3.
    Luego elimina el archivo TXT original.
    
    :param archivo_txt: Ruta del archivo TXT a procesar.
    :param bucket: Nombre del bucket de S3.
    :param ruta_s3_base: Ruta base en el bucket de S3 donde se subirá el archivo JSON.
    :param ruta_local_base: Ruta local base desde donde se están leyendo los archivos.
    """
    try:
        # Leer el archivo TXT
        df = pd.read_csv(archivo_txt, delimiter='\t')  # Asumiendo que los archivos son TSV
        
        # Transformar los datos
        df_transformado = transformar_datos(df)
        if df_transformado is None:
            logger.error(f"Error al transformar los datos para el archivo: {archivo_txt}")
            return
        
        # Obtener la ruta relativa del archivo respecto a la carpeta local base
        ruta_relativa = os.path.relpath(archivo_txt, ruta_local_base)
        
        # Definir la ruta del archivo JSON en S3 manteniendo la estructura de carpetas
        nombre_archivo_json = os.path.basename(archivo_txt).replace('.txt', '.json')
        ruta_s3 = os.path.join(ruta_s3_base, os.path.dirname(ruta_relativa), nombre_archivo_json)
        
        # Subir el DataFrame transformado a S3 como JSON
        guardar_json_s3(df_transformado, bucket, ruta_s3)
        
        # Eliminar el archivo TXT original
        os.remove(archivo_txt)
        #logger.info(f"Archivo TXT original eliminado: {archivo_txt}")
        
    except Exception as e:
        logger.error(f"Error al procesar el archivo {archivo_txt}: {e}")


