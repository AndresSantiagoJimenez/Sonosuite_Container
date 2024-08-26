import os
import stat
import paramiko
import logging
from settings import settings  # Asegúrate de importar correctamente tus configuraciones
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from loguru import logger

# Configura el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar las credenciales de AWS desde variables de entorno
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


def obtener_estructura_s3(bucket, prefix):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    estructura_s3 = {}
    daily_path_s3 = None

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    parts = key.split('/')
                    current_level = estructura_s3

                    for part in parts[:-1]:
                        if part not in current_level:
                            current_level[part] = {}
                        current_level = current_level[part]

                    current_level[parts[-1]] = size

        # Buscar 'Daily' en el prefijo dado
        if prefix:
            daily_path_s3 = buscar_carpeta_daily_en_s3(bucket, prefix)
            if daily_path_s3:
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path_s3}")

    except NoCredentialsError:
        logger.error("Credenciales de AWS no encontradas.")
    except PartialCredentialsError:
        logger.error("Credenciales de AWS incompletas.")
    except Exception as e:
        logger.error(f"Error al obtener la estructura de S3: {e}")

    return estructura_s3, daily_path_s3


def obtener_estructura_sftp(sftp, directorio):
    """
    Recorre recursivamente el directorio especificado en el servidor SFTP y obtiene su estructura.
    Además, busca la carpeta 'Daily' y retorna su ruta si existe.

    :param sftp: Instancia de `paramiko.SFTPClient` para conectar con el servidor SFTP.
    :param directorio: Ruta del directorio en el servidor SFTP.
    :return: Tupla con la estructura del directorio y la ruta de la carpeta 'Daily' si existe.
    """
    estructura_sftp = {}
    daily_path_sftp = None

    try:
        # Listar todos los elementos en el directorio actual
        items = sftp.listdir_attr(directorio)
        logger.info(f"Contenido del directorio {directorio}: {[item.filename for item in items]}")

        for item in items:
            item_path = os.path.join(directorio, item.filename).replace("\\", "/")

            if stat.S_ISDIR(item.st_mode):
                # Recorrer subdirectorios
                sub_estructura, sub_daily_path_sftp = obtener_estructura_sftp(sftp, item_path)
                estructura_sftp[item.filename] = sub_estructura

                # Si se encuentra 'Daily', actualizar la ruta
                if sub_daily_path_sftp:
                    daily_path_sftp = sub_daily_path_sftp
            else:
                # Guardar el tamaño del archivo
                estructura_sftp[item.filename] = item.st_size

        # Buscar la carpeta 'Daily' si aún no se ha encontrado
        if not daily_path_sftp:
            daily_path_sftp, _ = buscar_carpeta_daily(sftp, directorio)
            if daily_path_sftp:
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path_sftp}")

    except Exception as e:
        logger.error(f"Error al obtener la estructura del SFTP en {directorio}: {e}")
    
    return estructura_sftp, daily_path_sftp

def buscar_carpeta_daily(sftp, path):
    """
    Busca y retorna la ruta a la subcarpeta 'Daily' dentro del directorio especificado en el servidor SFTP.
    Además, verifica si dentro de 'Daily' existen las subcarpetas 'Ad-Supported', 'Prime' y 'Unlimited'.

    :param sftp: Instancia de `paramiko.SFTPClient` para conectar con el servidor SFTP.
    :param path: Ruta del directorio en el servidor SFTP.
    :return: Tupla con la ruta a la subcarpeta 'Daily' si existe y un diccionario con las subcarpetas encontradas.
    """
    daily_path = None
    subcarpetas_encontradas = {'Ad-Supported': False, 'Prime': False, 'Unlimited': False}
    
    try:
        # Listar todos los elementos en el directorio actual
        items = sftp.listdir_attr(path)
        logger.info(f"Contenido del directorio {path}: {[item.filename for item in items]}")

        # Buscar la carpeta 'Daily'
        for item in items:
            item_path = os.path.join(path, item.filename).replace("\\", "/")
            if item.filename == 'Daily' and stat.S_ISDIR(item.st_mode):
                daily_path = item_path
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path}")

                # Verificar subcarpetas dentro de 'Daily'
                try:
                    sub_items = sftp.listdir_attr(daily_path)
                    logger.info(f"Contenido de 'Daily': {[sub_item.filename for sub_item in sub_items]}")

                    for sub_item in sub_items:
                        sub_item_path = os.path.join(daily_path, sub_item.filename).replace("\\", "/")
                        if sub_item.filename in subcarpetas_encontradas and stat.S_ISDIR(sub_item.st_mode):
                            subcarpetas_encontradas[sub_item.filename] = True
                            logger.info(f"Subcarpeta '{sub_item.filename}' encontrada en: {sub_item_path}")
                        else:
                            logger.debug(f"Subcarpeta '{sub_item.filename}' no encontrada en: {sub_item_path}")
                
                except IOError as e:
                    logger.error(f"Error al listar subcarpetas en {daily_path}: {e}")
                
                break

        # Verificar si se encontraron todas las subcarpetas esperadas
        for subcarpeta, encontrada in subcarpetas_encontradas.items():
            if not encontrada:
                logger.error(f"No se encontró la subcarpeta '{subcarpeta}' en la carpeta 'Daily'.")

    except IOError as e:
        logger.error(f"Error al buscar la subcarpeta 'Daily' en el directorio {path}: {e}")
    
    return daily_path, subcarpetas_encontradas




def comparar_estructuras(estructura_sftp, estructura_s3):
    def comparar_dicts(dict1, dict2, path=""):
        faltantes = []
        extra = []

        # Compara las claves y tamaños
        all_keys = set(dict1.keys()).union(set(dict2.keys()))
        for key in all_keys:
            full_path = os.path.join(path, key).replace("\\", "/")
            if key not in dict2:
                faltantes.append(full_path)
            elif key not in dict1:
                extra.append(full_path)
            elif isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                sub_faltantes, sub_extra = comparar_dicts(dict1[key], dict2[key], full_path)
                faltantes.extend(sub_faltantes)
                extra.extend(sub_extra)
            elif dict1[key] != dict2[key]:
                faltantes.append(full_path)

        return faltantes, extra

    faltantes, extra = comparar_dicts(estructura_sftp, estructura_s3)
    return faltantes, extra



def main():
    """
    Función principal que ejecuta el proceso de comparación de estructuras entre SFTP y S3.
    """
    try:
        # Establecer conexión SFTP
        sftp_host = settings.sftp_host
        sftp_port = settings.sftp_port
        sftp_username = os.getenv('SFTP_USERNAME')
        sftp_password = os.getenv('SFTP_PASSWORD')
        directorio_sftp = settings.sftp_directorio_raiz

        # Establecer conexión con el servidor SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        logger.info("Conexión al servidor SFTP establecida exitosamente.")
        
        # Obtener estructura del SFTP
        estructura_sftp, daily_path_sftp = obtener_estructura_sftp(sftp, directorio_sftp)
        logger.info(f"Estructura SFTP obtenida: {estructura_sftp}")
        logger.info(f"Ruta 'Daily' en SFTP: {daily_path_sftp}")

        # Obtener estructura de S3
        bucket = settings.bucket_salida
        prefix = settings.Prefix
        estructura_s3, daily_path_s3 = obtener_estructura_s3(bucket, prefix)
        logger.info(f"Estructura S3 obtenida: {estructura_s3}")
        logger.info(f"Ruta 'Daily' en S3: {daily_path_s3}")

        # Comparar estructuras
        archivos_faltantes, archivos_extra = comparar_estructuras(estructura_sftp, estructura_s3)

        if not archivos_faltantes and not archivos_extra:
            logger.info("Las estructuras de SFTP y S3 coinciden.")
        else:
            if archivos_faltantes:
                logger.warning(f"Archivos faltantes en S3: {archivos_faltantes}")
            if archivos_extra:
                logger.warning(f"Archivos extra en S3: {archivos_extra}")

        # Cerrar la conexión SFTP
        sftp.close()
        transport.close()
        logger.info("Conexión SFTP cerrada correctamente.")

    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    main()
