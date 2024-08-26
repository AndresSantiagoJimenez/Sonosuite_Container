import boto3
import paramiko
import os
import stat
import pandas as pd
import zipfile
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from loguru import logger
from settings import settings
# Configura el logger de loguru
import loguru
logger = loguru.logger

# Configurar las credenciales de AWS desde variables de entorno
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


# Crear el directorio temporal si no existe
if not os.path.exists(settings.directorio_temporal):
    os.makedirs(settings.directorio_temporal)
    logger.info(f"Creado el directorio temporal: {settings.directorio_temporal}")

# pycode: Esta función lista todos los archivos en un bucket de S3 dado un prefijo.
def list_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files

# pycode: Esta función lista todos los archivos y directorios en un servidor SFTP dado un path remoto.
import posixpath  # Importa posixpath para asegurar rutas correctas en SFTP

def list_sftp_files(sftp_client, remote_path):
    """
    Esta función lista todos los archivos y directorios en un servidor SFTP dado un path remoto.
    """
    try:
        files = []
        logger.info(f"Listando archivos en el directorio SFTP: {remote_path}")
        for entry in sftp_client.listdir_attr(remote_path):
            if stat.S_ISDIR(entry.st_mode):  # Verifica si es un directorio
                logger.info(f"Directorio encontrado: {entry.filename}")
                # Utiliza posixpath.join para construir la ruta
                new_path = posixpath.join(remote_path, entry.filename)
                files.extend(list_sftp_files(sftp_client, new_path))
            else:
                logger.info(f"Archivo encontrado: {entry.filename}")
                files.append(posixpath.join(remote_path, entry.filename))
        return files
    except Exception as e:
        logger.error(f"Error listando archivos en SFTP: {e}")
        return []


# pycode: Esta función compara las estructuras de archivos entre S3 y SFTP y detecta diferencias.
def compare_structures(s3_files, sftp_files):
    # Extraer solo los nombres de archivo sin considerar la ruta completa
    s3_filenames = {file.split('/')[-1] for file in s3_files}
    sftp_filenames = {file.split('/')[-1] for file in sftp_files}
    
    # Archivos faltantes en cada sistema
    missing_in_s3 = sftp_filenames - s3_filenames
    missing_in_sftp = s3_filenames - sftp_filenames
    
    # Si hay diferencias, se registran en el log
    if missing_in_s3 or missing_in_sftp:
        logger.info("Las estructuras no son iguales.")
        
        if missing_in_s3:
            logger.info("Archivos presentes en SFTP pero faltantes en S3:")
            for item in sorted(missing_in_s3):
                logger.info(item)
                
        if missing_in_sftp:
            logger.info("Archivos presentes en S3 pero faltantes en SFTP:")
            for item in sorted(missing_in_sftp):
                logger.info(item)
    else:
        logger.info("Las estructuras son iguales.")




        
        
def descargar_y_descomprimir_archivos_faltantes(sftp, sftp_files, s3_files, directorio_temporal, subcarpetas_encontradas, max_reintentos=3):
    """
    Descarga y descomprime archivos ZIP faltantes desde las subcarpetas especificadas en SFTP si no están en S3.
    
    :param sftp: Instancia de `paramiko.SFTPClient`.
    :param sftp_files: Lista de archivos en SFTP.
    :param s3_files: Conjunto de archivos en S3.
    :param directorio_temporal: Directorio local donde se descargarán y descomprimirán los archivos.
    :param subcarpetas_encontradas: Diccionario indicando las subcarpetas a procesar (e.g., 'Ad-Supported': True).
    :param max_reintentos: Número máximo de intentos de descarga en caso de falla.
    """
    try:
        for carpeta, procesar in subcarpetas_encontradas.items():
            if procesar:
                logger.info(f"Procesando la subcarpeta: {carpeta}")
                for archivo in sftp_files:
                    if carpeta in archivo and archivo not in s3_files:
                        archivo_path = archivo.replace("\\", "/")
                        
                        # Crear estructura de directorios en el directorio temporal
                        relative_path = os.path.relpath(os.path.dirname(archivo_path), '/cxp-reporting/ZQLUC/sales')
                        directorio_local = os.path.join(directorio_temporal, relative_path)
                        os.makedirs(directorio_local, exist_ok=True)
                        
                        archivo_local = os.path.join(directorio_local, os.path.basename(archivo_path))

                        for intento in range(max_reintentos):
                            try:
                                logger.info(f"Intentando descargar: {archivo_path}")
                                logger.info(f"Guardando como: {archivo_local}")

                                # Descargar el archivo desde SFTP
                                sftp.get(archivo_path, archivo_local)
                                logger.info(f"Archivo ZIP descargado: {archivo_local}")

                                # Verificar si el archivo fue descargado correctamente
                                if not os.path.exists(archivo_local):
                                    logger.error(f"El archivo no se encuentra en la ruta esperada: {archivo_local}")
                                    continue

                                # Descomprimir el archivo ZIP en el directorio local
                                with zipfile.ZipFile(archivo_local, 'r') as zip_file:
                                    zip_file.extractall(directorio_local)
                                    logger.info(f"Archivo descomprimido en {directorio_local}")

                                # Eliminar el archivo ZIP después de descomprimir
                                os.remove(archivo_local)
                                logger.info(f"Archivo ZIP eliminado: {archivo_local}")
                                break  # Salir del bucle de reintento si se completa con éxito

                            except zipfile.BadZipFile as e:
                                logger.error(f"Error al descomprimir el archivo {archivo_local}: {e}")
                                break  # Si hay un error en el ZIP, no reintentar
                            except paramiko.SSHException as e:
                                logger.error(f"Error de conexión SSH: {e}")
                                if intento < max_reintentos - 1:
                                    logger.info(f"Reintentando ({intento + 1}/{max_reintentos})...")
                                else:
                                    logger.error(f"Máximo número de reintentos alcanzado para {archivo_local}")
                            except Exception as e:
                                logger.error(f"Error inesperado al descomprimir el archivo {archivo_local}: {e}")
                                break  # Detener si ocurre un error inesperado
            else:
                logger.info(f"Saltando la subcarpeta: {carpeta}")

    except Exception as e:
        logger.error(f"Error al descargar o descomprimir archivos: {e}")

def asegurar_directorio(directorio):
    """
    Crea el directorio si no existe.
    """
    if not os.path.exists(directorio):
        os.makedirs(directorio)
        logger.info(f"Creado el directorio: {directorio}")
        

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

def main():
    """
    Función principal que ejecuta el proceso de comparación de estructuras entre SFTP y S3.
    """
    try:
        # Configuración de S3
        s3_bucket_name = settings.bucket_salida
        s3_prefix = settings.Prefix

        # Establecer conexión SFTP
        sftp_hostname = settings.sftp_host
        sftp_port = settings.sftp_port
        sftp_username = os.getenv('SFTP_USERNAME')
        sftp_password = os.getenv('SFTP_PASSWORD')

        # Asegurarse de que el directorio temporal exista
        asegurar_directorio(settings.directorio_temporal)

        # Establecer conexión con el servidor SFTP
        transport = paramiko.Transport((sftp_hostname, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Obtener listas de archivos
        s3_files = list_s3_files(s3_bucket_name, s3_prefix)
        logger.info(f"S3: Estructura: {s3_files}")
        sftp_files = list_sftp_files(sftp, settings.sftp_directorio_raiz)
        logger.info(f"FTP: Estructura: {sftp_files}")

        # Comparar estructuras
        compare_structures(s3_files, sftp_files)

        """# Descargar y descomprimir archivos faltantes
        subcarpetas_encontradas = {'Ad-Supported': True, 'Prime': False, 'Unlimited': True}
        archivos_descargados = descargar_y_descomprimir_archivos_faltantes(
            sftp, sftp_files, set(s3_files), settings.directorio_temporal, subcarpetas_encontradas
        )"""

        """Procesar y guardar localmente los archivos descomprimidos como JSON
        logger.info(f"Archivos en el directorio {settings.directorio_temporal}: {os.listdir(settings.directorio_temporal)}")
        for archivo in os.listdir(settings.directorio_temporal):
            archivo_path = os.path.join(settings.directorio_temporal, archivo)
            if os.path.isfile(archivo_path) and archivo.endswith('.txt'):
                logger.info(f"Procesando archivo: {archivo_path}")
                procesar_y_guardar_localmente(archivo_path)
            else:
                logger.info(f"Omitiendo: {archivo_path} (No es un archivo .txt)")"""
                
                # Procesar y guardar localmente los archivos descomprimidos como JSON
        """for root, dirs, files in os.walk(settings.directorio_temporal):
            for archivo in files:
                if archivo.endswith('.txt'):
                    archivo_path = os.path.join(root, archivo)
                    logger.info(f"Procesando archivo: {archivo_path}")
                    procesar_y_guardar_localmente(archivo_path)
                else:
                    logger.info(f"Omitiendo: {archivo} (No es un archivo .txt)")"""

        # Cerrar la conexión SFTP
        sftp.close()
        transport.close()

    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    # Configura el logger de loguru
    logger.add("logs/pruebas_archivos.log", rotation="10 MB", retention="10 days")
    main()


        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
"""
# pycode: Función principal que ejecuta el proceso de comparación de estructuras entre SFTP y S3.
def main():
    
    Función principal que ejecuta el proceso de comparación de estructuras entre SFTP y S3.
    
    try:
        # Configuración de S3
        s3_bucket_name = settings.bucket_salida  # Asegúrate de que esto sea solo el nombre del bucket
        s3_prefix = settings.Prefix  # El prefijo (si lo hay) debe estar separado

        # Establecer conexión SFTP
        sftp_hostname = settings.sftp_host
        sftp_port = settings.sftp_port
        sftp_username = os.getenv('SFTP_USERNAME')
        sftp_password = os.getenv('SFTP_PASSWORD')

        # Establecer conexión con el servidor SFTP
        transport = paramiko.Transport((sftp_hostname, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Obtener listas de archivos
        s3_files = list_s3_files(s3_bucket_name, s3_prefix)
        logger.info(f"S3: Estructura: {s3_files}")
        sftp_files = list_sftp_files(sftp, settings.sftp_directorio_raiz)
        logger.info(f"FTP: Estructura: {sftp_files}")

        # Comparar estructuras
        compare_structures(s3_files, sftp_files)
    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    # Configura el logger de loguru
    logger.add("logs/pruebas_archivos.log", rotation="10 MB", retention="10 days")
    main()

"""



        
"""        
# Configuración de S3
s3_bucket_name = ‘tu-bucket-s3’
s3_prefix = ‘carpeta-a-comparar/’  # Opcional, si deseas comparar solo una subcarpeta
# Configuración de SFTP
sftp_hostname = ‘tu-servidor-sftp’
sftp_username = ‘tu-usuario’
sftp_password = ‘tu-contraseña’
sftp_remote_path = ‘/ruta/en/el/servidor/’
# Obtener listas de archivos
s3_files = list_s3_files(s3_bucket_name, s3_prefix)
sftp_client = paramiko.SSHClient()
sftp_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
sftp_client.connect(sftp_hostname, username=sftp_username, password=sftp_password)
sftp = sftp_client.open_sftp()
sftp_files = list_sftp_files(sftp, sftp_remote_path)
sftp.close()
sftp_client.close()
# Comparar estructuras
compare_structures(s3_files, sftp_files)"""




"""def compare_structures(s3_files, sftp_files):
    s3_set = set(s3_files)
    sftp_set = set(sftp_files)
    missing_in_s3 = sftp_set - s3_set
    missing_in_sftp = s3_set - sftp_set
    if missing_in_s3 or missing_in_sftp:
        print("Las estructuras no son iguales.")
        if missing_in_s3:
            print("Archivos/directorios faltantes en S3:")
            for item in missing_in_s3:
                print(item)
        if missing_in_sftp:
            print("Archivos/directorios faltantes en SFTP:")
            for item in missing_in_sftp:
                print(item)
    else:
        print("Las estructuras son iguales.")"""