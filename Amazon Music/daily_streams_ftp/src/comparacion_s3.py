import os
from loguru import logger
from config.settings import settings
import paramiko
from src.s3_utils import list_s3_files, upload_missing_files_to_s3
from src.sftp_utils import list_sftp_files

def compare_structures(s3_files, sftp_files, sftp_base_path='/cxp-reporting/ZQLUC/sales'):
    """
    Compara las estructuras de archivos entre S3 y SFTP y detecta archivos faltantes en S3.
    Omite archivos cuyo nombre contenga "Summary_Statement".
    
    :param s3_files: Lista de archivos en S3.
    :param sftp_files: Lista de archivos en SFTP.
    :param sftp_base_path: Ruta base en SFTP para extraer la ruta relativa.
    :return: Conjunto de archivos faltantes en S3.
    """
    # Excluir archivos con "Summary_Statement" en el nombre
    sftp_files_filtered = [file for file in sftp_files if "Summary_Statement" not in os.path.basename(file)]
    
    # Extraer las rutas relativas de los archivos en SFTP
    sftp_relative_paths = {os.path.relpath(file, sftp_base_path).replace("\\", "/") for file in sftp_files_filtered}
    logger.info(f"Rutas relativas en SFTP: {sftp_relative_paths}")

    # Extraer los nombres de archivos de S3
    s3_filenames = {os.path.basename(file) for file in s3_files}
    logger.info(f"Archivos en S3: {s3_filenames}")

    # Archivos que están en SFTP pero no en S3
    missing_in_s3 = {file for file in sftp_relative_paths if os.path.basename(file) not in s3_filenames}

    if missing_in_s3:
        logger.info("Archivos presentes en SFTP pero faltantes en S3:")
        for item in sorted(missing_in_s3):
            logger.info(item)
    else:
        logger.info("Todos los archivos en SFTP están presentes en S3.")

    return missing_in_s3

import os
import zipfile
import paramiko
from loguru import logger
from src.s3_utils import list_s3_files, upload_missing_files_to_s3
from src.sftp_utils import list_sftp_files
from src.file_comparisons import compare_structures
from src.file_transformer import procesar_y_guardar_en_s3
from src.file_uploader import asegurar_directorio, limpiar_directorio_temporal

def descargar_y_descomprimir_archivos_faltantes(sftp, sftp_files, s3_files, directorio_temporal, subcarpetas_encontradas, max_reintentos=3):
    """
    Descarga y descomprime archivos ZIP faltantes desde las subcarpetas especificadas en SFTP si no están en S3.
    """
    missing_in_s3, _ = compare_structures(s3_files, sftp_files)
    patrones_a_omitir = ['Summary_Statement']  # Patrones a omitir
    try:
        for carpeta, procesar in subcarpetas_encontradas.items():
            if procesar:
                logger.info(f"Procesando la subcarpeta: {carpeta}")
                for archivo in sftp_files:
                    # Extraer el nombre del archivo para verificar los patrones
                    nombre_archivo = os.path.basename(archivo)

                    # Verificar si el archivo debe ser omitido
                    if any(patron in nombre_archivo for patron in patrones_a_omitir):
                        logger.info(f"Omitiendo el archivo: {nombre_archivo}")
                        continue

                    if carpeta in archivo and nombre_archivo in missing_in_s3:
                        archivo_path = archivo.replace("\\", "/")
                        relative_path = os.path.relpath(os.path.dirname(archivo_path), '/cxp-reporting/ZQLUC/sales')
                        directorio_local = os.path.join(directorio_temporal, relative_path)
                        os.makedirs(directorio_local, exist_ok=True)
                        archivo_local = os.path.join(directorio_local, nombre_archivo)

                        for intento in range(max_reintentos):
                            try:
                                logger.info(f"Intentando descargar: {archivo_path}")
                                logger.info(f"Guardando como: {archivo_local}")
                                # Descargar el archivo desde SFTP
                                sftp.get(archivo_path, archivo_local)
                                logger.info(f"Archivo ZIP descargado: {archivo_local}")

                                if not os.path.exists(archivo_local):
                                    logger.error(f"El archivo no se encuentra en la ruta esperada: {archivo_local}")
                                    continue

                                # Descomprimir el archivo ZIP
                                try:
                                    with zipfile.ZipFile(archivo_local, 'r') as zip_file:
                                        zip_file.extractall(directorio_local)
                                        logger.info(f"Archivo descomprimido en {directorio_local}")
                                except zipfile.BadZipFile as e:
                                    logger.error(f"Error al descomprimir el archivo {archivo_local}: {e}")
                                    break  # No reintentar si hay un error en el archivo ZIP

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
        # Inicio del proceso
        print("Iniciando el proceso...")
        
        # Obtener listas de archivos en S3
        s3_files = list_s3_files(settings.BUCKET_NAME, settings.S3_PREFIX)
        #logger.info(f"S3: Estructura: {s3_files}")

        # Establecer conexión con el servidor SFTP
        transport = paramiko.Transport((settings.SFTP_HOST, settings.SFTP_PORT))
        transport.connect(username=settings.SFTP_USERNAME, password=settings.SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Obtener listas de archivos en SFTP
        sftp_files = list_sftp_files(sftp, settings.SFTP_DIRECTORIO_RAIZ)
        #logger.info(f"SFTP: Estructura: {sftp_files}")

        # Comparar estructuras
        compare_structures(s3_files, sftp_files)

        # Asegurarse de que el directorio temporal exista
        asegurar_directorio(settings.DIRECTORIO_TEMPORAL)

        # Descargar y descomprimir archivos faltantes
        subcarpetas_encontradas = {'Ad-Supported': True, 'Prime': True, 'Unlimited': True}
        descargar_y_descomprimir_archivos_faltantes(
            sftp, sftp_files, set(s3_files), settings.DIRECTORIO_TEMPORAL, subcarpetas_encontradas
        )

        # Procesar y subir los archivos descomprimidos como JSON directamente a S3
        for root, dirs, files in os.walk(settings.DIRECTORIO_TEMPORAL):
            for archivo in files:
                if archivo.endswith('.txt'):
                    archivo_path = os.path.join(root, archivo)
                    logger.info(f"Procesando archivo: {archivo_path}")
                    
                    # Llamar a la nueva función que sube los archivos directamente a S3
                    procesar_y_guardar_en_s3(
                        archivo_path,
                        settings.BUCKET_NAME,
                        settings.S3_PREFIX_RAW,
                        settings.DIRECTORIO_TEMPORAL  # Base local para obtener la ruta relativa
                    )
                else:
                    logger.info(f"Omitiendo: {archivo} (No es un archivo .txt)")

        # Subir solo los archivos faltantes a S3
        upload_missing_files_to_s3(
            settings.DIRECTORIO_TEMPORAL, 
            settings.BUCKET_NAME, 
            settings.S3_PREFIX,  # 'src/raw/' como prefijo en S3
            set(s3_files)  # Archivos que ya existen en S3
        )


        # Limpiar el directorio temporal después de subir los archivos a S3
        #limpiar_directorio_temporal(settings.DIRECTORIO_TEMPORAL)

        # Cerrar la conexión SFTP
        sftp.close()
        transport.close()

        # Fin del proceso
        logger.info("Proceso completado con éxito.")
        #+print("Proceso completado con éxito.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del proceso: {e}")

if __name__ == "__main__":
    main()