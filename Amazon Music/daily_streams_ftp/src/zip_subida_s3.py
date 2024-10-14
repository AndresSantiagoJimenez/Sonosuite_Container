import zipfile
import os
import pandas as pd
import logging

# Configuración básica del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directorios
DIRECTORIO_ZIP = r"C:\Users\Laboral_Santiago\Documents\Personal_Santiago\Faltantes_Sonosuite\US_DAILY\ADS"
DIRECTORIO_DESTINO_JSON = r"C:\Users\Laboral_Santiago\Documents\Personal_Santiago\Faltantes_Sonosuite\US_DAILY\ADS\json"

def descomprimir_archivos_zip(directorio_zip, directorio_destino):
    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino)

    for archivo in os.listdir(directorio_zip):
        if archivo.endswith(".zip"):
            ruta_zip = os.path.join(directorio_zip, archivo)
            try:
                with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                    zip_ref.extractall(directorio_destino)
                    logger.info(f"Archivo {archivo} descomprimido en {directorio_destino}")
            except zipfile.BadZipFile:
                logger.error(f"El archivo {archivo} no es un ZIP válido.")
            except Exception as e:
                logger.error(f"Error al descomprimir {archivo}: {e}")

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
        df = df.rename(columns=lambda x: x.upper())  # Convertir todos los nombres a mayúsculas
        return df
    except Exception as e:
        logger.error(f"Error al transformar los datos: {e}")
        return None


def procesar_archivos_txt(directorio_txt, directorio_destino):
    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino)

    for archivo in os.listdir(directorio_txt):
        if archivo.endswith(".txt"):
            ruta_txt = os.path.join(directorio_txt, archivo)
            try:
                # Cambiar 'error_bad_lines' por 'on_bad_lines'
                df = pd.read_csv(ruta_txt, sep='\t', on_bad_lines='skip', engine='python')  # Ignora líneas problemáticas
                
                df_transformado = transformar_datos(df)
                if df_transformado is not None:
                    nombre_archivo_json = archivo.replace('.txt', '.json')
                    ruta_json = os.path.join(directorio_destino, nombre_archivo_json)
                    df_transformado.to_json(ruta_json, orient='records', lines=True)
                    logger.info(f"Archivo {nombre_archivo_json} guardado en {directorio_destino}")
                else:
                    logger.error(f"Error al transformar el archivo {archivo}")
            except Exception as e:
                logger.error(f"Error al procesar el archivo {archivo}: {e}")


# Ejemplo de uso
descomprimir_archivos_zip(DIRECTORIO_ZIP, DIRECTORIO_DESTINO_JSON)
procesar_archivos_txt(DIRECTORIO_DESTINO_JSON, os.path.join(DIRECTORIO_DESTINO_JSON, "Ad-Supported")) #Prime, Unlimited, Ad-Supported
