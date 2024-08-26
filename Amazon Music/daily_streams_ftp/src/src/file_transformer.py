import pandas as pd
import zipfile
import os
from loguru import logger

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

def guardar_json_local(df, ruta_json):
    """
    Guarda el DataFrame en formato JSON en una ruta local.
    :param df: DataFrame a guardar.
    :param ruta_json: Ruta del archivo JSON donde se guardará el DataFrame.
    """
    try:
        json_data = df.to_json(orient='records', lines=True)
        with open(ruta_json, 'w') as file:
            file.write(json_data)
        logger.info(f"Archivo JSON guardado localmente: {ruta_json}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo JSON localmente: {e}")

def procesar_y_guardar_localmente(archivo_txt):
    """
    Procesa un archivo TXT delimitado por tabulaciones, lo transforma y lo guarda como JSON localmente.
    Luego elimina el archivo TXT original.
    :param archivo_txt: Ruta del archivo TXT a procesar.
    """
    try:
        # Leer el archivo TXT
        df = pd.read_csv(archivo_txt, delimiter='\t')  # Asumiendo que los archivos son TSV
        logger.info(f"Archivo TXT cargado: {archivo_txt}")
        # Transformar los datos
        df_transformado = transformar_datos(df)
        if df_transformado is None:
            logger.error(f"Error al transformar los datos para el archivo: {archivo_txt}")
            return
        # Definir la ruta del archivo JSON
        ruta_json = archivo_txt.replace('.txt', '.json')
        # Guardar el DataFrame transformado localmente como JSON
        guardar_json_local(df_transformado, ruta_json)
        # Eliminar el archivo TXT original
        os.remove(archivo_txt)
        logger.info(f"Archivo TXT original eliminado: {archivo_txt}")
    except Exception as e:
        logger.error(f"Error al procesar el archivo {archivo_txt}: {e}")

