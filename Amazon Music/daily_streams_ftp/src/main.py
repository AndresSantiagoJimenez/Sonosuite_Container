import os
import zipfile
import pandas as pd
import paramiko
from loguru import logger
from src.s3_utils import list_s3_files, upload_missing_files_to_s3
from src.sftp_utils import list_sftp_files
from src.file_comparisons import compare_structures
from src.file_transformer import upload_and_transform_txt_files_to_s3
from src.file_uploader import asegurar_directorio, limpiar_directorio_temporal
from src.sftp_downloader import descargar_y_descomprimir_archivos_faltantes
from config.settings import settings

def main():
    # Inicio del proceso
    logger.info("Iniciando el proceso...")

    # Obtener listas de archivos en S3
    try:
        s3_files = list_s3_files(settings.BUCKET_NAME, settings.S3_PREFIX)
        #logger.info(f"Archivos en S3: {s3_files}")
    except Exception as e:
        logger.error(f"Error obteniendo lista de archivos de S3: {e}")
        return

    # Establecer conexión con el servidor SFTP
    try:
        transport = paramiko.Transport((settings.SFTP_HOST, settings.SFTP_PORT))
        transport.connect(username=settings.SFTP_USERNAME, password=settings.SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        #logger.info("Conexión SFTP establecida correctamente")

        # Obtener listas de archivos en SFTP
        sftp_files = list_sftp_files(sftp, settings.SFTP_DIRECTORIO_RAIZ)
        #logger.info(f"Archivos en SFTP: {sftp_files}")
    except Exception as e:
        logger.error(f"Error conectando al servidor SFTP: {e}")
        return

    try:
        # Comparar estructuras
        missing_in_s3 = compare_structures(s3_files, sftp_files)
        #logger.info(f"Archivos faltantes en S3: {missing_in_s3}")

        # Asegurarse de que el directorio temporal exista
        asegurar_directorio(settings.DIRECTORIO_TEMPORAL)
        #logger.info(f"Directorio temporal asegurado: {settings.DIRECTORIO_TEMPORAL}")

        # Descargar y descomprimir archivos faltantes
        subcarpetas_encontradas = {'Ad-Supported': True, 'Prime': True, 'Unlimited': True}
        archivos_descargados = descargar_y_descomprimir_archivos_faltantes(
            sftp, sftp_files, s3_files, settings.DIRECTORIO_TEMPORAL, subcarpetas_encontradas, settings.SFTP_DIRECTORIO_RAIZ
        )
        logger.info(f"Archivos descargados: {archivos_descargados}")

        # Validar que los archivos descargados existan
        #for root, dirs, files in os.walk(settings.DIRECTORIO_TEMPORAL):
        #    if files:
        #        logger.info(f"Archivos encontrados en {root}: {files}")
        #    else:
        #        logger.info(f"No se encontraron archivos en {root}")

# Procesar y subir archivos .txt a S3
        for root, dirs, files in os.walk(settings.DIRECTORIO_TEMPORAL):
            logger.info(f"Archivos encontrados en {root}: {files}")
            for archivo in files:
                archivo_path = os.path.join(root, archivo)

                if archivo.lower().endswith('.txt'):
                    logger.info(f"Procesando archivo: {archivo_path}")
                    
                    # Llamar a la función con los parámetros correctos
                    upload_and_transform_txt_files_to_s3(
                        archivo_path,              # Ruta completa del archivo .txt
                        settings.BUCKET_NAME,      # Nombre del bucket en S3
                        settings.S3_PREFIX_RAW,    # Prefijo de S3 (src/sales)
                        settings.DIRECTORIO_TEMPORAL  # Directorio temporal local
                    )
                else:
                    logger.info(f"Omitiendo archivo no .txt: {archivo}")

        # Subir archivos faltantes a S3
        upload_missing_files_to_s3(
            settings.DIRECTORIO_TEMPORAL, 
            settings.BUCKET_NAME, 
            settings.S3_PREFIX,  
            set(s3_files)
        )
        logger.info("Archivos subidos a S3 correctamente")

        # Limpiar el directorio temporal después de subir los archivos a S3
        limpiar_directorio_temporal(settings.DIRECTORIO_TEMPORAL)
        logger.info("Directorio temporal limpiado")

    except Exception as e:
        logger.error(f"Ocurrió un error durante el proceso: {e}")
    finally:
        # Cerrar la conexión SFTP
        if 'sftp' in locals():
            sftp.close()
            logger.info("Conexión SFTP cerrada")
        if 'transport' in locals():
            transport.close()
            logger.info("Transporte SFTP cerrado")

    # Fin del proceso
    logger.info("Proceso completado con éxito.")

if __name__ == "__main__":
    main()
