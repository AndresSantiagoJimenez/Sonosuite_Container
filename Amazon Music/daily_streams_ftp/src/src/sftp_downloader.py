import os
import zipfile
from loguru import logger
from .file_comparisons import compare_structures

def descargar_y_descomprimir_archivos_faltantes(sftp, sftp_files, s3_files, directorio_temporal, subcarpetas_encontradas, sftp_base_path):
    archivos_descargados = []

    # Comparar estructuras de archivos entre SFTP y S3 para encontrar archivos faltantes en S3
    missing_in_s3 = compare_structures(s3_files, sftp_files, sftp_base_path)
    logger.info(f"Archivos faltantes en S3: {missing_in_s3}")

    for subcarpeta, encontrada in subcarpetas_encontradas.items():
        if encontrada:
            logger.info(f"Procesando la subcarpeta: {subcarpeta}")

            # Filtrar archivos de la subcarpeta que faltan en S3, omitiendo "Summary_Statement"
            archivos_para_descargar = [
                archivo for archivo in sftp_files 
                if archivo.endswith('.zip') 
                and 'Summary_Statement' not in archivo 
                and os.path.relpath(archivo, sftp_base_path).replace("\\", "/") in missing_in_s3
            ]

            logger.info(f"Archivos para descargar en la subcarpeta {subcarpeta}: {archivos_para_descargar}")

            for archivo in archivos_para_descargar:
                archivo_sin_extension = os.path.basename(archivo).replace('.zip', '')
                partes_nombre = archivo_sin_extension.split('_')

# Verificar si es un archivo de AmazonMP3 y obtener país y servicio
                if 'AmazonMP3' in os.path.basename(archivo) and len(partes_nombre) >= 4:
                    pais = partes_nombre[-3].replace('.txt', '')  # Obtener el país y eliminar .txt
                    servicio = partes_nombre[2]  # Obtener el servicio

                    # Crear la ruta completa para guardar el archivo en "Daily"
                    ruta_local = os.path.join(directorio_temporal)  # Solo usar "Daily"
                    os.makedirs(ruta_local, exist_ok=True)

                    # Ruta para el archivo ZIP
                    archivo_zip_local = os.path.join(ruta_local, os.path.basename(archivo))

                else:
                    # Si no se puede extraer del nombre, intentar usar la ruta
                    ruta_subcarpeta = os.path.dirname(archivo)
                    partes = ruta_subcarpeta.split('/')

                    # Verificar que haya suficientes partes
                    if len(partes) < 3:  # Al menos debe haber el país y la subcarpeta
                        logger.error(f"La ruta {ruta_subcarpeta} no contiene la estructura esperada.")
                        continue

                    # Suponiendo que la estructura es: /cxp-reporting/ZQLUC/sales/{pais}/Daily/{servicio}
                    pais = partes[-3].replace('.txt', '')  # Remover .txt si está presente
                    servicio = partes[-1]  # Tomar el servicio de la última parte

                # Asegurarse de que el servicio mantenga el sufijo completo
                if 'ROW' in pais:  # Verifica si el país contiene "ROW"
                    pais = '_'.join(pais.split('_')[-2:])  # Extrae el sufijo después de "ROW"
                    servicio = f"{servicio}"  # Agrega el sufijo completo, para que quede así ROW_NA/Daily/Unlimited

                # Crear la ruta completa
                ruta_local = os.path.join(directorio_temporal, pais, "Daily", servicio)
                os.makedirs(ruta_local, exist_ok=True)

                archivo_zip_local = os.path.join(ruta_local, os.path.basename(archivo))

                try:
                    # Descargar el archivo ZIP desde el servidor SFTP
                    logger.info(f"Descargando archivo: {archivo}")
                    sftp.get(archivo, archivo_zip_local)
                    logger.info(f"Archivo descargado: {archivo_zip_local}")

                    # Descomprimir el archivo
                    with zipfile.ZipFile(archivo_zip_local, 'r') as zip_ref:
                        zip_ref.extractall(ruta_local)
                        logger.info(f"Archivo ZIP {archivo_zip_local} descomprimido en {ruta_local}")

                        # Listar y verificar los archivos extraídos
                        archivos_descomprimidos = zip_ref.namelist()
                        for archivo in archivos_descomprimidos:
                            logger.info(f"Archivo descomprimido: {archivo}")

                            # Verificar si es un archivo .txt
                            if archivo.endswith('.txt'):
                                archivo_txt_local = os.path.join(ruta_local, os.path.basename(archivo).replace('.txt', ''))  # Remover .txt
                                logger.info(f"Archivo .txt descomprimido: {archivo_txt_local}")
                                archivos_descargados.append((archivo_zip_local, archivo_txt_local))

                except Exception as e:
                    logger.error(f"Error descargando/descomprimiendo el archivo {archivo}: {e}")

    return archivos_descargados
