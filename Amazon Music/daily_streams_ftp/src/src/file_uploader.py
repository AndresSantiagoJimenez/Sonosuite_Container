import shutil
import os
from loguru import logger

def asegurar_directorio(directorio):
    if not os.path.exists(directorio):
        os.makedirs(directorio)
        logger.info(f"Creado el directorio: {directorio}")

def limpiar_directorio_temporal(directorio):
    try:
        if os.path.exists(directorio):
            shutil.rmtree(directorio)
            logger.info(f"Directorio {directorio} limpiado correctamente.")
        else:
            logger.warning(f"Directorio {directorio} no existe, nada que limpiar.")
    except Exception as e:
        logger.error(f"Error al limpiar el directorio {directorio}: {e}")
