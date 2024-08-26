import os
import zipfile

def es_realmente_un_zip(archivo):
    """Verifica si el archivo tiene la firma de un ZIP."""
    with open(archivo, 'rb') as f:
        firma = f.read(2)
        return firma == b'PK'

def descomprimir_y_eliminar_zip(directorio):
    for carpeta_raiz, subcarpetas, archivos in os.walk(directorio):
        for archivo in archivos:
            if archivo.endswith('.zip'):
                ruta_zip = os.path.join(carpeta_raiz, archivo)
                print(f"Archivo .zip encontrado: {ruta_zip}")
                
                # Verifica si el archivo realmente es un ZIP
                if es_realmente_un_zip(ruta_zip):
                    try:
                        with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                            zip_ref.extractall(carpeta_raiz)
                            print(f"Descomprimido: {ruta_zip}")
                        os.remove(ruta_zip)
                        print(f"Eliminado: {ruta_zip}")
                    except zipfile.BadZipFile:
                        print(f"Error: {ruta_zip} no es un archivo ZIP válido.")
                else:
                    print(f"Advertencia: {ruta_zip} no es un archivo ZIP real.")

# Ejemplo de uso
directorio_principal = r'src\Data'
descomprimir_y_eliminar_zip(directorio_principal)
