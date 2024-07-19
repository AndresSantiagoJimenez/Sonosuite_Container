import os
import boto3
import pandas as pd
from io import StringIO
from loguru import logger
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, TokenRetrievalError, ClientError
import paramiko
import zipfile
from settings import settings  # Asegúrate de importar correctamente tus configuraciones

# Configurar las credenciales de AWS desde variables de entorno
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Configurar la sesión y el cliente de S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Crear el directorio temporal si no existe
if not os.path.exists(settings.directorio_temporal):
    os.makedirs(settings.directorio_temporal)
    logger.info(f"Creado el directorio temporal: {settings.directorio_temporal}")

def cargar_archivos_local(directorio):
    try:
        if not os.path.exists(directorio):
            logger.error(f"El directorio no existe: {directorio}")
            return []

        logger.info(f"Buscando archivos en el directorio: {directorio}")
        archivos_ingesta = [os.path.join(directorio, f) for f in os.listdir(directorio) if f.endswith(('.json', '.csv', '.txt'))]
        logger.info(f"Archivos encontrados: {archivos_ingesta}")

        if not archivos_ingesta:
            logger.error("No se encontraron archivos en el directorio especificado.")
            return []

        dataframes = []
        for archivo in archivos_ingesta:
            try:
                if archivo.endswith('.json'):
                    with open(archivo, 'r', encoding='utf-8') as file:
                        df = pd.read_json(file, orient='records')
                elif archivo.endswith('.csv'):
                    df = pd.read_csv(archivo)
                elif archivo.endswith('.txt'):
                    df = pd.read_csv(archivo, delimiter='\t')
                dataframes.append((df, archivo))
            except PermissionError as pe:
                logger.error(f"Permiso denegado al intentar leer el archivo {archivo}. Verifica los permisos: {pe}")
            except FileNotFoundError as fnf:
                logger.error(f"Archivo no encontrado: {archivo}. Verifica la ruta: {fnf}")
            except Exception as e:
                logger.error(f"Error al leer el archivo {archivo}: {e}. Saltando este archivo.")
        return dataframes
    except Exception as e:
        logger.error(f"Error al buscar archivos en el directorio {directorio}: {e}")
        return []

def transformar_datos(df):
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

def comparar_estructuras_s3(df_local, bucket, key):
    try:
        # Descargar el archivo JSON desde S3 para comparar estructuras
        response = s3.get_object(Bucket=bucket, Key=key)
        df_s3 = pd.read_json(response['Body'], orient='records')
        
        # Verificar que las columnas coincidan
        if list(df_local.columns) != list(df_s3.columns):
            logger.error(f"Estructura del archivo en S3 ({key}) no coincide con el archivo local.")
            return False
        
        # Aquí podrías agregar más validaciones si es necesario (por ejemplo, tipos de datos)
        
        logger.info(f"Estructura validada en S3: {key}")
        return True
    except Exception as e:
        logger.error(f"Error al comparar estructuras con S3: {e}")
        return False

def validar_archivo_s3(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Archivo validado en S3: {key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error(f"Archivo no encontrado en S3: {key}")
            return False
        else:
            logger.error(f"Error al validar archivo en S3: {e}")
            return False
    except Exception as e:
        logger.error(f"Error inesperado al validar archivo en S3: {e}")
        return False

def guardar_json_s3(df, bucket, key):
    try:
        json_buffer = StringIO()
        df.to_json(json_buffer, orient='records')
        
        # Validar estructura antes de guardar en S3
        if comparar_estructuras_s3(df, bucket, key):
            s3.put_object(Bucket=bucket, Key=key, Body=json_buffer.getvalue().encode('utf-8'))
            logger.info(f"Archivo JSON guardado en S3: {key}")
            return validar_archivo_s3(bucket, key)
        else:
            logger.error(f"No se pudo validar la estructura del archivo en S3: {key}")
            return False
    except (NoCredentialsError, PartialCredentialsError, TokenRetrievalError) as e:
        logger.error(f"Error de credenciales: {e}")
    except Exception as e:
        logger.error(f"Error al guardar en S3: {e}")
        return False

def descargar_archivos_sftp(directorio_remoto, directorio_destino):
    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((settings.sftp_host, settings.sftp_port))
        transport.connect(username=os.getenv('SFTP_USERNAME'), password=os.getenv('SFTP_PASSWORD'))
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        def buscar_carpeta_daily(path):
            logger.info(f"Buscando en: {path}")
            try:
                items = sftp.listdir(path)
                for item in items:
                    item_path = os.path.join(path, item).replace("\\", "/")
                    if sftp.stat(item_path).st_mode & 0o170000 == 0o040000:  # Es una carpeta
                        logger.info(f"Encontrada carpeta: {item_path}")
                        if item.lower() == 'daily':
                            return item_path
                        else:
                            daily_path = buscar_carpeta_daily(item_path)
                            if daily_path:
                                return daily_path
            except IOError as e:
                logger.error(f"Error al listar el directorio {path}: {e}")
            return None
        
        def descargar_y_descomprimir_archivos_subcarpeta(path, subcarpeta, directorio_destino, carpeta_anterior):
            try:
                if sftp.stat(path).st_mode & 0o170000 == 0o040000:  # Es una carpeta
                    daily_path = buscar_carpeta_daily(path)
                    if daily_path:
                        subcarpeta_path = os.path.join(daily_path, subcarpeta).replace("\\", "/")
                        if sftp.stat(subcarpeta_path).st_mode & 0o170000 == 0o040000:  # Es una carpeta
                            archivos = sftp.listdir(subcarpeta_path)
                            for archivo in archivos:
                                if archivo.endswith('.zip'):
                                    archivo_remoto = os.path.join(subcarpeta_path, archivo).replace("\\", "/")
                                    archivo_local = os.path.join(directorio_destino, carpeta_anterior, subcarpeta, archivo)
                                    if not os.path.exists(os.path.dirname(archivo_local)):
                                        os.makedirs(os.path.dirname(archivo_local))
                                    sftp.get(archivo_remoto, archivo_local)
                                    logger.info(f'Descargado: {archivo} desde {archivo_remoto} a {archivo_local}')
                                    
                                    # Descomprimir el archivo zip
                                    try:
                                        with zipfile.ZipFile(archivo_local, 'r') as zip_ref:
                                            zip_ref.extractall(os.path.dirname(archivo_local))
                                            logger.info(f'Descomprimido: {archivo}')
                                            os.remove(archivo_local)
                                            logger.info(f'Eliminado archivo zip: {archivo}')
                                    except zipfile.BadZipFile:
                                        logger.error(f"Archivo zip corrupto: {archivo}")
                                        
                descargar_y_descomprimir_archivos_subcarpeta(directorio_remoto, 'ventas', directorio_destino, 'ventas')
            except Exception as e:
                logger.error(f"Error al descargar archivos SFTP: {e}")
            finally:
                if sftp:
                    sftp.close()
                if transport:
                    transport.close()

    except Exception as e:
        logger.error(f"Error al establecer conexión SFTP: {e}")
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def limpiar_carpeta_data():
    try:
        directorio_data = 'src/Data'  # Reemplaza 'src/Data' con la carpeta Data correcta
        for archivo in os.listdir(directorio_data):
            archivo_path = os.path.join(directorio_data, archivo)
            if os.path.isfile(archivo_path):
                os.remove(archivo_path)
                logger.info(f"Archivo eliminado: {archivo_path}")
        logger.info("Todos los archivos en la carpeta Data han sido eliminados.")
    except Exception as e:
        logger.error(f"Error al eliminar archivos en la carpeta Data: {e}")

def procesar_archivos():
    try:
        # Descargar archivos desde SFTP
        descargar_archivos_sftp(settings.directorio_temporal, settings.directorio_local)
        
        # Cargar archivos locales y procesarlos
        dataframes = cargar_archivos_local(settings.directorio_local)
        for df, archivo in dataframes:
            df_transformado = transformar_datos(df)
            if df_transformado is not None:
                # Guardar datos transformados en S3
                nombre_archivo_s3 = f"transformados/{os.path.basename(archivo)}"
                guardado_correcto = guardar_json_s3(df_transformado, settings.bucket_salida, nombre_archivo_s3)
                if guardado_correcto:
                    logger.info(f"Datos transformados guardados correctamente en S3: {nombre_archivo_s3}")
                else:
                    logger.error(f"Error al guardar datos transformados en S3: {nombre_archivo_s3}")
                    return  # Terminar la ejecución si hay un error en la carga a S3

        logger.info("Proceso completado exitosamente.")
    except Exception as e:
        logger.error(f"Error general en el procesamiento de archivos: {e}")
    finally:
        # Llamar a la función de limpieza después de completar el procesamiento
        limpiar_carpeta_data()
        logger.info("Finalizando ejecución del DAG.")

# Ejecutar la función principal
procesar_archivos()
