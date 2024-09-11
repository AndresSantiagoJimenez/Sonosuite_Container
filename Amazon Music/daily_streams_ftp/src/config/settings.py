
#bucket_salida = 'sns-amazonmusic-trends'  # Nombre del bucket de salida
#Prefix = 'src/sales/'

#bucket_salida = 'sns-amazonmusic-trends'  # Nombre del bucket de salida
#Prefix = 'src/prueba/'


# Variables del servidor SFTP y del directorio temporal
#sftp_host = 'prod.reporting.amazonmusiccatalog.com'
#sftp_port = 22

#sftp_username = ''
#sftp_password = ''
#directorio_temporal = 'src/Data'
#directorio_local = 'src/Data'
#sftp_directorio_raiz = '/cxp-reporting/ZQLUC/sales'
#sftp_directorio_raiz = '/cxp-reporting/ZQLUC/sales/'

import os

class Settings:
    # AWS S3
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    BUCKET_NAME = 'sns-amazonmusic-trends'  # Nombre del bucket de salida
    S3_PREFIX = 'src/raw/'  # Prefijo en S3
    S3_PREFIX_RAW = 'src/sales/'  # Prefijo en S3 para raw

    # SFTP
    SFTP_HOST = 'prod.reporting.amazonmusiccatalog.com'
    SFTP_PORT = int(os.getenv('SFTP_PORT', 22))  # Puerto SFTP (predeterminado: 22)
    SFTP_USERNAME = os.getenv('SFTP_USERNAME')
    SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')
    SFTP_DIRECTORIO_RAIZ = '/cxp-reporting/ZQLUC/sales/'  # Directorio raíz en SFTP

    # Directorios
    DIRECTORIO_TEMPORAL = 'src/Data'
    DIRECTORIO_LOCAL = 'src/Data'

settings = Settings()

