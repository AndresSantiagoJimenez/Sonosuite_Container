import os
from loguru import logger

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

