import os
import logging
from settings import settings  # Asegúrate de importar correctamente tus configuraciones
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configurar las credenciales de AWS desde variables de entorno
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
# Configura el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    s3_client = boto3.client('s3')
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

        # Buscar 'Daily' en el prefijo dado
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

def main():
    """
    Función principal que ejecuta el proceso de comparación de estructuras entre SFTP y S3.
    """
    try:
        # Obtener estructura de S3
        bucket = settings.bucket_salida
        prefix = settings.Prefix
        estructura_s3, daily_path_s3 = obtener_estructura_s3(bucket, prefix)
        logger.info(f"Estructura S3 obtenida: {estructura_s3}")
        logger.info(f"Ruta 'Daily' en S3: {daily_path_s3}")

    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    main()
