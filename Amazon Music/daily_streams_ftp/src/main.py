import os
import zipfile
import paramiko
from loguru import logger
from src.s3_utils import list_s3_files, upload_missing_files_to_s3
from src.sftp_utils import list_sftp_files
from src.file_comparisons import compare_structures
from src.file_transformer import procesar_y_guardar_localmente
from src.file_uploader import asegurar_directorio, limpiar_directorio_temporal


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
    # Obtén los archivos que faltan en S3 utilizando la función compare_structures
    missing_in_s3, _ = compare_structures(s3_files, sftp_files)
    # Lista de patrones de archivos a omitir
    patrones_a_omitir = ['Summary_Statement']
    try:
        for carpeta, procesar in subcarpetas_encontradas.items():
            if procesar:
                logger.info(f"Procesando la subcarpeta: {carpeta}")
                for archivo in sftp_files:
                    # Verificar si el archivo debe ser omitido
                    if any(patron in archivo for patron in patrones_a_omitir):
                        logger.info(f"Omitiendo el archivo: {archivo}")
                        continue
                    if carpeta in archivo and archivo.split('/')[-1] in missing_in_s3:
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
                                try:
                                    with zipfile.ZipFile(archivo_local, 'r') as zip_file:
                                        zip_file.extractall(directorio_local)
                                        logger.info(f"Archivo descomprimido en {directorio_local}")
                                except zipfile.BadZipFile as e:
                                    logger.error(f"Error al descomprimir el archivo {archivo_local}: {e}")
                                    break  # Si hay un error en el ZIP, no reintentar"""
                                # Eliminar el archivo ZIP después de descomprimir
                                #os.remove(archivo_local)
                                #logger.info(f"Archivo ZIP eliminado: {archivo_local}")
                                #break  # Salir del bucle de reintento si se completa con éxito
                            except paramiko.SSHException as e:
                                logger.error(f"Error de conexión SSH: {e}")
                                if intento < max_reintentos - 1:
                                    logger.info(f"Reintentando ({intento + 1}/{max_reintentos})...")
                                else:
                                    logger.error(f"Máximo número de reintentos alcanzado para {archivo_local}")
                            except Exception as e:
                                logger.error(f"Error inesperado al descargar el archivo {archivo_local}: {e}")
                                break  # Detener si ocurre un error inesperado
            else:
                logger.info(f"Saltando la subcarpeta: {carpeta}")
    except Exception as e:
        logger.error(f"Error al descargar y descomprimir archivos: {e}")

from config.settings import settings

def main():
    try:
        # Obtener listas de archivos en S3
        s3_files = list_s3_files(settings.BUCKET_NAME, settings.S3_PREFIX)
        logger.info(f"S3: Estructura: {s3_files}")

        # Establecer conexión con el servidor SFTP
        transport = paramiko.Transport((settings.SFTP_HOST, settings.SFTP_PORT))
        transport.connect(username=settings.SFTP_USERNAME, password=settings.SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Obtener listas de archivos en SFTP
        sftp_files = list_sftp_files(sftp, settings.SFTP_DIRECTORIO_RAIZ)
        logger.info(f"SFTP: Estructura: {sftp_files}")

        # Comparar estructuras
        compare_structures(s3_files, sftp_files)

        # Asegurarse de que el directorio temporal exista
        asegurar_directorio(settings.DIRECTORIO_TEMPORAL)

        # Descargar y descomprimir archivos faltantes
        subcarpetas_encontradas = {'Ad-Supported': True, 'Prime': False, 'Unlimited': True}
        descargar_y_descomprimir_archivos_faltantes(
            sftp, sftp_files, set(s3_files), settings.DIRECTORIO_TEMPORAL, subcarpetas_encontradas
        )

        # Procesar y guardar localmente los archivos descomprimidos como JSON
        for root, dirs, files in os.walk(settings.DIRECTORIO_TEMPORAL):
            for archivo in files:
                if archivo.endswith('.txt'):
                    archivo_path = os.path.join(root, archivo)
                    logger.info(f"Procesando archivo: {archivo_path}")
                    procesar_y_guardar_localmente(archivo_path)
                else:
                    logger.info(f"Omitiendo: {archivo} (No es un archivo .txt)")

        # Subir solo los archivos faltantes a S3
        upload_missing_files_to_s3(settings.DIRECTORIO_TEMPORAL, settings.BUCKET_NAME, settings.S3_PREFIX, settings.S3_PREFIX_RAW, set(s3_files))

        # Limpiar el directorio temporal después de subir los archivos a S3
        limpiar_directorio_temporal(settings.DIRECTORIO_TEMPORAL)

        # Cerrar la conexión SFTP
        sftp.close()
        transport.close()
    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":

    main()


