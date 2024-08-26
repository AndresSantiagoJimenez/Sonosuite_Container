import os
import stat
from loguru import logger
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import paramiko
from settings import settings  # Asegúrate de importar correctamente tus configuraciones

# Configura el logger de loguru
logger.add("logs/comparacion_s3_sftp.log", rotation="10 MB", retention="10 days")

# Configurar las credenciales de AWS desde variables de entorno
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def buscar_carpeta_daily(sftp, path):
    daily_path = None
    subcarpetas_encontradas = {'Ad-Supported': False, 'Prime': False, 'Unlimited': False}
    
    try:
        items = sftp.listdir_attr(path)
        logger.info(f"Contenido del directorio {path}: {[item.filename for item in items]}")

        for item in items:
            item_path = os.path.join(path, item.filename).replace("\\", "/")
            if item.filename == 'Daily' and stat.S_ISDIR(item.st_mode):
                daily_path = item_path
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path}")

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

        for subcarpeta, encontrada in subcarpetas_encontradas.items():
            if not encontrada:
                logger.error(f"No se encontró la subcarpeta '{subcarpeta}' en la carpeta 'Daily'.")

    except IOError as e:
        logger.error(f"Error al buscar la subcarpeta 'Daily' en el directorio {path}: {e}")
    
    return daily_path, subcarpetas_encontradas

def obtener_estructura_sftp(sftp, directorio):
    estructura_sftp = {}
    daily_path_sftp = None

    try:
        items = sftp.listdir_attr(directorio)
        logger.info(f"Contenido del directorio {directorio}: {[item.filename for item in items]}")

        for item in items:
            item_path = os.path.join(directorio, item.filename).replace("\\", "/")

            if stat.S_ISDIR(item.st_mode):
                sub_estructura, sub_daily_path_sftp = obtener_estructura_sftp(sftp, item_path)
                estructura_sftp[item.filename] = sub_estructura

                if sub_daily_path_sftp:
                    daily_path_sftp = sub_daily_path_sftp
            else:
                estructura_sftp[item.filename] = item.st_size

        if not daily_path_sftp:
            daily_path_sftp, _ = buscar_carpeta_daily(sftp, directorio)
            if daily_path_sftp:
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path_sftp}")

    except Exception as e:
        logger.error(f"Error al obtener la estructura del SFTP en {directorio}: {e}")
    
    return estructura_sftp, daily_path_sftp

def comparar_estructuras(s3_estructura, sftp_estructura, path=""):
    diferencias = []

    for key in s3_estructura:
        if key not in sftp_estructura:
            diferencias.append(f"{path}/{key} está en S3 pero no en SFTP.")
            logger.warning(f"{path}/{key} está en S3 pero no en SFTP.")
        elif isinstance(s3_estructura[key], dict):
            diferencias.extend(comparar_estructuras(s3_estructura[key], sftp_estructura[key], f"{path}/{key}"))
        else:
            if s3_estructura[key] != sftp_estructura[key]:
                diferencias.append(f"{path}/{key} tiene tamaños diferentes entre S3 y SFTP.")
                logger.warning(f"{path}/{key} tiene tamaños diferentes entre S3 y SFTP.")
    
    for key in sftp_estructura:
        if key not in s3_estructura:
            diferencias.append(f"{path}/{key} está en SFTP pero no en S3.")
            logger.warning(f"{path}/{key} está en SFTP pero no en S3.")

    return diferencias


def buscar_carpeta_daily_en_s3(bucket, prefix):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    daily_path = None

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix_info in page['CommonPrefixes']:
                    if 'Daily' in prefix_info['Prefix']:
                        daily_path = prefix_info['Prefix']
                        break
                if daily_path:
                    break

    except NoCredentialsError:
        logger.error("Credenciales de AWS no encontradas.")
    except PartialCredentialsError:
        logger.error("Credenciales de AWS incompletas.")
    except Exception as e:
        logger.error(f"Error al buscar la carpeta 'Daily' en S3: {e}")

    return daily_path

def listar_subcarpetas_s3(bucket, prefix):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    subcarpetas = []

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix_info in page['CommonPrefixes']:
                    subcarpetas.append(prefix_info['Prefix'])
    except NoCredentialsError:
        logger.error("Credenciales de AWS no encontradas.")
    except PartialCredentialsError:
        logger.error("Credenciales de AWS incompletas.")
    except Exception as e:
        logger.error(f"Error al listar subcarpetas en S3: {e}")

    return subcarpetas


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

        if prefix:
            daily_path_s3 = buscar_carpeta_daily_en_s3(bucket, prefix)
            if daily_path_s3:
                logger.info(f"Carpeta 'Daily' encontrada en: {daily_path_s3}")
                subcarpetas = listar_subcarpetas_s3(bucket, daily_path_s3)
                logger.info(f"Subcarpetas en 'Daily': {subcarpetas}")

    except NoCredentialsError:
        logger.error("Credenciales de AWS no encontradas.")
    except PartialCredentialsError:
        logger.error("Credenciales de AWS incompletas.")
    except Exception as e:
        logger.error(f"Error al obtener la estructura de S3: {e}")

    return estructura_s3, daily_path_s3


def normalizar_ruta(ruta):
    return os.path.normpath(ruta).replace('\\', '/').rstrip('/')

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

        # Definir las rutas antes de normalizarlas
        ruta_sftp = settings.sftp_directorio_raiz
        ruta_s3 = settings.Prefix

        ruta_sftp = normalizar_ruta(ruta_sftp)
        ruta_s3 = normalizar_ruta(ruta_s3)
        
        if ruta_sftp == ruta_s3:
            print(f"Las rutas son iguales: {ruta_sftp}")
        else:
            print(f"Diferencias encontradas: SFTP ({ruta_sftp}) y S3 ({ruta_s3})")
        
        # Obtener estructura de S3
        estructura_s3, daily_path_s3 = obtener_estructura_s3(settings.bucket_salida, settings.Prefix)
        logger.info(f"Estructura S3 obtenida: {estructura_s3}")
        logger.info(f"Ruta 'Daily' en S3: {daily_path_s3}")

        # Obtener estructura de SFTP
        estructura_sftp, daily_path_sftp = obtener_estructura_sftp(sftp, settings.sftp_directorio_raiz)
        logger.info(f"Estructura SFTP obtenida: {estructura_sftp}")
        logger.info(f"Ruta 'Daily' en SFTP: {daily_path_sftp}")

        # Comparar estructuras
        diferencias = comparar_estructuras(estructura_s3, estructura_sftp)
        if diferencias:
            logger.info("Diferencias encontradas entre S3 y SFTP:")
            for diferencia in diferencias:
                logger.info(diferencia)
        else:
            logger.info("No se encontraron diferencias entre S3 y SFTP.")

        sftp.close()

    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    main()

