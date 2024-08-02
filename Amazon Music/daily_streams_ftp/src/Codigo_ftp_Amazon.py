import os
import boto3
import pandas as pd
import stat
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
#s3 = boto3.client('s3')

# Crear el directorio temporal si no existe
if not os.path.exists(settings.directorio_temporal):
    os.makedirs(settings.directorio_temporal)
    logger.info(f"Creado el directorio temporal: {settings.directorio_temporal}")

def obtener_estructura_sftp(sftp, directorio):
    """
    Obtiene la estructura de carpetas y subcarpetas en un directorio del servidor SFTP,
    y busca la subcarpeta 'Daily'.
    """
    estructura = {}
    daily_path = None
    try:
        #logger.info(f"Listando atributos en el directorio: {directorio}")
        for item in sftp.listdir_attr(directorio):
            item_path = os.path.join(directorio, item.filename).replace("\\", "/")
            #logger.info(f"Procesando: SFTP{item_path}")
            if sftp.stat(item_path).st_mode & 0o170000 == 0o040000:  # Es un directorio
                logger.info(f"Directorio encontrado: {item_path}")
                estructura[item.filename], daily_path = obtener_estructura_sftp(sftp, item_path)
                if daily_path:
                    logger.info(f"Carpeta 'Daily' encontrada en: {daily_path}")
            else:
                estructura[item.filename] = None
        
        # Buscar la carpeta 'Daily' en el directorio actual
        daily_path = buscar_carpeta_daily(sftp, directorio)
        if daily_path:
            logger.info(f"Carpeta 'Daily' encontrada en: {daily_path}")

        logger.info(f"Estructura obtenida para {directorio}: {estructura}")
    except Exception as e:
        logger.error(f"Error al obtener la estructura del SFTP en {directorio}: {e}")
    return estructura, daily_path


def obtener_estructura_s3(bucket, prefix):
    """
    Obtiene la estructura de carpetas y subcarpetas en un bucket de S3,
    y busca la carpeta 'Daily'.

    :param bucket: Nombre del bucket en S3.
    :param prefix: Prefijo para listar objetos en S3.
    :return: Tupla con el diccionario de la estructura del bucket y la ruta a la carpeta 'Daily' en S3 si existe.
    """
    estructura = {}
    daily_path_s3 = None
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        for prefix in response.get('CommonPrefixes', []):
            sub_prefix = prefix['Prefix']
            sub_estructura, daily_path_sub_s3 = obtener_estructura_s3(bucket, sub_prefix)
            estructura[os.path.basename(sub_prefix)] = sub_estructura
            if daily_path_sub_s3:
                daily_path_s3 = daily_path_sub_s3
        
        # Buscar la carpeta 'Daily' en el prefijo actual de S3
        if 'Daily/' in prefix:
            daily_path_s3 = prefix
            logger.info(f"Carpeta 'Daily' encontrada en S3: {daily_path_s3}")
        
    except ClientError as e:
        logger.error(f"Error al listar objetos en S3: {e}")
    
    return estructura, daily_path_s3



def comparar_estructuras(estructura_sftp, estructura_s3):
    """
    Compara la estructura de carpetas y subcarpetas entre SFTP y S3.

    :param estructura_sftp: Estructura de carpetas del servidor SFTP.
    :param estructura_s3: Estructura de carpetas en S3.
    :return: True si las estructuras coinciden, False en caso contrario.
    """
    if set(estructura_sftp.keys()) != set(estructura_s3.keys()):
        return False
    for carpeta in estructura_sftp.keys():
        if carpeta not in estructura_s3:
            return False
        if not comparar_estructuras(estructura_sftp[carpeta], estructura_s3[carpeta]):
            return False
    return True

def cargar_archivos_local(directorio):
    """
    Carga archivos locales desde el directorio especificado y los convierte en DataFrames de pandas.

    :param directorio: Ruta del directorio que contiene los archivos.
    :return: Lista de tuplas (DataFrame, nombre_archivo) para cada archivo cargado.
    """
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
                logger.error(f"Permiso denegado al intentar leer el archivo {archivo}: {pe}")
            except FileNotFoundError as fnf:
                logger.error(f"Archivo no encontrado: {archivo}: {fnf}")
            except Exception as e:
                logger.error(f"Error al leer el archivo {archivo}: {e}")
        return dataframes
    except Exception as e:
        logger.error(f"Error al buscar archivos en el directorio {directorio}: {e}")
        return []

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

def guardar_json_s3(df, bucket, key):
    """
    Guarda el DataFrame en formato JSON en un bucket de S3.

    :param df: DataFrame a guardar.
    :param bucket: Nombre del bucket en S3.
    :param key: Clave del objeto en S3.
    """
    try:
        json_data = df.to_json(orient='records', lines=True)
        s3.put_object(Bucket=bucket, Key=key, Body=json_data)
        logger.info(f"Archivo JSON guardado en S3: {key}")
        if not validar_archivo_s3(bucket, key):
            logger.error(f"El archivo en S3 no fue encontrado después de guardar: {key}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo en S3: {e}")

def validar_archivo_s3(bucket, key):
    """
    Valida la existencia de un archivo en S3.

    :param bucket: Nombre del bucket en S3.
    :param key: Clave del objeto en S3.
    :return: True si el archivo existe, False en caso contrario.
    """
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

def limpiar_carpeta_data():
    """
    Elimina todos los archivos en el directorio de datos temporal.

    :return: None
    """
    try:
        directorio_data = settings.directorio_temporal
        for archivo in os.listdir(directorio_data):
            archivo_path = os.path.join(directorio_data, archivo)
            if os.path.isfile(archivo_path):
                os.remove(archivo_path)
                logger.info(f"Archivo eliminado: {archivo_path}")
        logger.info("Todos los archivos en la carpeta Data han sido eliminados.")
    except Exception as e:
        logger.error(f"Error al eliminar archivos en la carpeta Data: {e}")

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

def descargar_y_descomprimir_archivos_subcarpeta(sftp, ruta_directorio):
    """
    Busca la subcarpeta 'Daily', descarga y descomprime archivos ZIP desde el servidor SFTP, 
    guardándolos en el directorio local especificado en la configuración.

    :param sftp: Instancia de `paramiko.SFTPClient` para conectar con el servidor SFTP.
    :param ruta_directorio: Ruta del directorio en el servidor SFTP donde se buscará la subcarpeta 'Daily'.
    """
    try:
        # Buscar la subcarpeta 'Daily'
        ruta_subcarpeta, _ = buscar_carpeta_daily(sftp, ruta_directorio)
        
        if not ruta_subcarpeta:
            logger.error(f"No se pudo encontrar la subcarpeta 'Daily' en {ruta_directorio}.")
            return
        
        # Descargar y descomprimir archivos ZIP en la subcarpeta 'Daily'
        archivos = sftp.listdir(ruta_subcarpeta)
        logger.info(f"Archivos encontrados en la subcarpeta 'Daily': {archivos}")
        
        for archivo in archivos:
            archivo_path = os.path.join(ruta_subcarpeta, archivo).replace("\\", "/")
            if archivo.lower().endswith('.zip'):
                archivo_local = os.path.join(settings.directorio_local, archivo)
                sftp.get(archivo_path, archivo_local)
                logger.info(f"Archivo ZIP descargado: {archivo_local}")
                try:
                    with zipfile.ZipFile(archivo_local, 'r') as zip_file:
                        zip_file.extractall(settings.directorio_local)
                        logger.info(f"Archivo descomprimido en {settings.directorio_local}")
                except zipfile.BadZipFile as e:
                    logger.error(f"Error al descomprimir el archivo {archivo_local}: {e}")
                except Exception as e:
                    logger.error(f"Error inesperado al descomprimir el archivo {archivo_local}: {e}")
    except IOError as e:
        logger.error(f"Error al descargar o descomprimir archivos: {e}")
        
        
def descargar_y_procesar_archivos_sftp():
    """
    Descarga y procesa archivos desde el servidor SFTP, valida la estructura con S3 y guarda los datos transformados en S3.

    :return: None
    """
    sftp = None
    transport = None

    try:
        # Establecer conexión SFTP
        sftp_host = settings.sftp_host
        sftp_port = settings.sftp_port
        sftp_username = os.getenv('SFTP_USERNAME')
        sftp_password = os.getenv('SFTP_PASSWORD')

        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        logger.info("Conectado al servidor SFTP.")
        
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info("Cliente SFTP inicializado.")
        logger.info("Conexión SFTP establecida y cliente inicializado. Procediendo con la obtención de estructura.")
        
        # Obtener estructura del SFTP
        sftp_raiz = settings.sftp_directorio_raiz
        estructura_sftp = obtener_estructura_sftp(sftp, sftp_raiz)
        logger.info(f"Estructura SFTP obtenida: {estructura_sftp}")

        # Obtener estructura de S3
        estructura_s3, daily_path_s3 = obtener_estructura_s3(settings.bucket_salida, settings.Prefix)
        logger.info(f"Estructura S3 obtenida: {estructura_s3}, Daily path: {daily_path_s3}")
        
        if not comparar_estructuras(estructura_sftp, estructura_s3):
            logger.info("Las estructuras no coinciden.")
            
            # Obtener subcarpetas y procesar archivos
            subcarpetas = [f for f in sftp.listdir(sftp_raiz) if sftp.stat(os.path.join(sftp_raiz, f)).st_mode & 0o170000 == 0o040000]
            logger.info(f"Subcarpetas encontradas: {subcarpetas}")
            
            for subcarpeta in subcarpetas:
                ruta_subcarpeta = os.path.join(sftp_raiz, subcarpeta).replace("\\", "/")
                logger.info(f"Descargando y descomprimiendo archivos de la subcarpeta: {ruta_subcarpeta}")
                descargar_y_descomprimir_archivos_subcarpeta(sftp, ruta_subcarpeta)
                logger.info(f"Descarga y descompresión completa para la subcarpeta: {ruta_subcarpeta}")
            
            # Cargar y transformar archivos
            dataframes = cargar_archivos_local(settings.directorio_local)
            
            if dataframes:
                for df, archivo in dataframes:
                    logger.info(f"Transformando datos del archivo: {archivo}")
                    df = transformar_datos(df)
                    if df is not None:
                        key = f"{os.path.basename(archivo)}.json"
                        logger.info(f"Guardando datos transformados en S3 con clave: {key}")
                        guardar_json_s3(df, settings.bucket_salida, key)
                        logger.info(f"Datos guardados en S3 exitosamente para el archivo: {archivo}")
            
            # Limpiar carpeta temporal
            logger.info("Limpiando carpeta temporal.")
            limpiar_carpeta_data()
            logger.info("Carpeta temporal limpia.")
        else:
            logger.info("Las estructuras coinciden. No es necesario procesar los archivos.")
    
    except paramiko.ssh_exception.SSHException as e:
        logger.error(f"Error de conexión SSH: {e}")
    except paramiko.sftp.SFTPError as e:
        logger.error(f"Error de SFTP: {e}")
    except Exception as e:
        logger.error(f"Error en la función descargar_y_procesar_archivos_sftp: {e}")
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def main():
    """
    Función principal que ejecuta el proceso de descarga y procesamiento de archivos desde el servidor SFTP.

    :return: None
    """
    descargar_y_procesar_archivos_sftp()

if __name__ == "__main__":
    main()
