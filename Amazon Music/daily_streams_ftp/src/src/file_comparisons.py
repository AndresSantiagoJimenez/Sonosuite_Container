from loguru import logger

def compare_structures(s3_files, sftp_files):
    """
    Compara las estructuras de archivos entre S3 y SFTP y detecta diferencias.
    :param s3_files: Lista de archivos en S3.
    :param sftp_files: Lista de archivos en SFTP.
    :return: Conjuntos de archivos faltantes en S3 y SFTP.
    """
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
    # Devolver los conjuntos de archivos faltantes
    return missing_in_s3, missing_in_sftp
