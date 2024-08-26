import paramiko
import posixpath
import stat
from loguru import logger
from config import settings

def list_sftp_files(sftp_client, remote_path, base_dir="Daily"):
    files = []
    norm_remote_path = posixpath.normpath(remote_path)
    logger.info(f"Verificando ruta: {norm_remote_path}")
    try:
        for entry in sftp_client.listdir_attr(remote_path):
            entry_path = posixpath.join(remote_path, entry.filename)
            logger.info(f"Procesando entrada: {entry_path}")
            if stat.S_ISDIR(entry.st_mode):
                logger.info(f"Directorio encontrado: {entry.filename}")
                files.extend(list_sftp_files(sftp_client, entry_path, base_dir))
            else:
                if base_dir in posixpath.normpath(entry_path).split(posixpath.sep):
                    logger.info(f"Archivo encontrado dentro de {base_dir}: {entry.filename}")
                    files.append(entry_path)
                else:
                    logger.info(f"Archivo fuera de {base_dir}: {entry.filename}")
        return files
    except Exception as e:
        logger.error(f"Error listando archivos en SFTP: {e}")
        return []
