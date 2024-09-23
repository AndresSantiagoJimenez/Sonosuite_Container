import shutil
import os
from loguru import logger

def asegurar_directorio(directorio_base, subdirectorio=None):
    """
    Asegura que el directorio (y cualquier subdirectorio opcional) exista, 
    y si no, lo crea. Usa os.path.join para rutas seguras.
    
    Args:
    - directorio_base: La ruta base del directorio a asegurar.
    - subdirectorio: Subdirectorio opcional dentro del directorio_base.
    """
    try:
        # Construir la ruta completa de manera segura
        if subdirectorio:
            directorio = os.path.join(directorio_base, subdirectorio)
        else:
            directorio = directorio_base
        
        # Verifica si el directorio ya existe
        if not os.path.exists(directorio):
            # Si no existe, lo crea
            os.makedirs(directorio)
            logger.info(f"Directorio creado: {directorio}")
        else:
            logger.info(f"Directorio ya existe: {directorio}")
    except Exception as e:
        logger.error(f"Error al asegurar el directorio {directorio}: {e}")
        raise

def limpiar_directorio_temporal(directorio):
    try:
        if os.path.exists(directorio):
            shutil.rmtree(directorio)
            logger.info(f"Directorio {directorio} limpiado correctamente.")
        else:
            logger.warning(f"Directorio {directorio} no existe, nada que limpiar.")
    except Exception as e:
        logger.error(f"Error al limpiar el directorio {directorio}: {e}")
