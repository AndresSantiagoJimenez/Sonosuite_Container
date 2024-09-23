import boto3
import os
from config.settings import settings

# Parámetros
S3_BUCKET_NAME = 'sns-amazonmusic-trends'  # Reemplaza con el nombre de tu bucket
S3_PREFIX_RAW = 'src/sales/'  # Prefijo donde se almacenan los datos
DAILY_FOLDER = 'daily'
DAILY_FOLDER_RENAMED = 'Daily'

# Inicializar el cliente S3
s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    

def listar_objetos_prefijo(bucket, prefix):
    """
    Lista los objetos que comienzan con un prefijo dado en un bucket de S3.
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return response.get('Contents', [])

def renombrar_directorio(bucket, old_key, new_key):
    """
    Renombra un directorio en S3 copiando el contenido a un nuevo nombre y eliminando el original.
    """
    # Obtener todos los objetos en la carpeta a renombrar
    objetos = listar_objetos_prefijo(bucket, old_key)
    
    if not objetos:
        print(f"No se encontró la carpeta '{old_key}'.")
        return
    
    for obj in objetos:
        # Generar la nueva clave para cada archivo
        new_obj_key = obj['Key'].replace(old_key, new_key, 1)
        
        # Copiar el objeto a la nueva ubicación
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': obj['Key']},
            Key=new_obj_key
        )
        # Eliminar el objeto original
        s3.delete_object(Bucket=bucket, Key=obj['Key'])
    
    print(f"Directorio renombrado de '{old_key}' a '{new_key}'.")

def buscar_y_renombrar_carpeta_daily():
    """
    Busca la carpeta 'daily' en cada país y la renombra a 'Daily'.
    """
    # Listar todos los países dentro de 'src/sales/'
    objetos = listar_objetos_prefijo(S3_BUCKET_NAME, S3_PREFIX_RAW)
    
    # Crear un conjunto con los países (directorios de primer nivel)
    paises = set(os.path.dirname(obj['Key']).split('/')[2] for obj in objetos if len(obj['Key'].split('/')) > 3)
    
    for pais in paises:
        old_daily_path = f"{S3_PREFIX_RAW}{pais}/{DAILY_FOLDER}/"
        new_daily_path = f"{S3_PREFIX_RAW}{pais}/{DAILY_FOLDER_RENAMED}/"
        
        # Verificar si la carpeta 'daily' existe y renombrarla
        if listar_objetos_prefijo(S3_BUCKET_NAME, old_daily_path):
            print(f"Renombrando carpeta 'daily' para el país: {pais}")
            renombrar_directorio(S3_BUCKET_NAME, old_daily_path, new_daily_path)
        else:
            print(f"No se encontró la carpeta 'daily' para el país: {pais}")

if __name__ == '__main__':
    buscar_y_renombrar_carpeta_daily()
