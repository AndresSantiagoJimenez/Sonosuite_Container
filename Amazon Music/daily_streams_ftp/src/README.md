# Proyecto Amazon_Innventa

Este proyecto está diseñado para procesar datos desde un servidor SFTP de Amazon y cargarlos en AWS S3. Además, incluye integración con Airflow para la ejecución programada del procesamiento de datos.

## Estructura del proyecto

El proyecto está estructurado de la siguiente manera:

Amazon_Innventa/
│
├── config/
│   ├── settings.py
│   └── Diccionario.json        # Configuraciones del proyecto (ajustes, credenciales, rutas)
│
├── src/
│   ├── s3_utils.py        # Funciones relacionadas con AWS S3
│   ├── sftp_utils.py      # Funciones relacionadas con SFTP
│   ├── file_transformer.py # Funciones para transformar y procesar archivos
│   └── file_uploader.py   # Funciones para subir archivos a S3
│
├── logs/
│   └── pruebas_archivos.log
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .dockerignore
└── main.py                # Script principal

1. **src/**: Contiene el script principal 'main.py' para procesar archivos desde SFTP, la configuración en settings/, y otros archivos necesarios.
2. **docker-compose.yml**: Configuración para Docker Compose, usado para manejar múltiples contenedores Docker.
3. **Dockerfile**: Archivo para construir la imagen Docker del proyecto.
4. **requirements.txt**: Archivo de requisitos con las dependencias del proyecto.
5. **.dockerignore**: Archivo para especificar los archivos y carpetas que Docker debe ignorar al construir la imagen.

## Requisitos Previos

1. **Python 3.8+**: Asegúrate de tener instalado Python 3.8 o una versión superior.
2. **Boto3**: El SDK de AWS para Python.
3. **Pandas**: Una biblioteca de manipulación y análisis de datos.
4. **Loguru**: Biblioteca de registro para una gestión eficiente de logs en Python.
5. **Paramiko**: Implementación de Python de SSH para conectarse al servidor SFTP.
6. **Airflow**: Para la ejecución programada del procesamiento de datos.

Asegúrate de tener estas dependencias instaladas antes de ejecutar el proyecto.

## Instalación

1. Clona este repositorio:

    ```sh
    git clone https://github.com/tu_usuario/Diccionario_Datos_Amazon.git
    cd Diccionario_Datos_Amazon
    ```

2. Crea un entorno virtual y actívalo:

    ```sh
    python -m venv venv
    source venv/bin/activate  # En Windows usa `venv\Scripts\activate`
    ```

3. Instala las dependencias:

    ```sh
    pip install -r requirements.txt
    ```

4. Configurar variables de entorno:

    ```sh
    export SFTP_USERNAME=tu_usuario
    export SFTP_PASSWORD=tu_contraseña
    ```

5. Construir la imagen Docker:

    ```sh
    docker-compose build
    ```

6. Iniciar el contenedor Docker:

    ```sh
    docker-compose up
    ```

Esto iniciará el contenedor Docker con el script `Codigo_ftp_Amazon.py`, encargado de procesar los archivos desde el servidor SFTP de Amazon y cargarlos en AWS S3.

## Uso

1. Asegúrate de que el directorio de datos de origen existe en el servidor SFTP de Amazon.

2. Ejecuta el script principal:

    ```sh
    python src/Codigo_ftp_Amazon.py
    ```

## Descripción del Código

### Importación de Módulos

```python
import os
import boto3
import pandas as pd
from io import StringIO
from loguru import logger
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, TokenRetrievalError, ClientError
import paramiko
import zipfile
from settings import settings  # Asegúrate de importar correctamente tus configuraciones
```

### Configuración y Inicialización de Boto3

```python
boto3.setup_default_session(profile_name='sonosuite')
s3 = boto3.client('s3')
```

### Función para Cargar Archivos Locales

```python
def cargar_archivos_local(directorio):
    ...
```

### Función para Transformar Datos

```python
def transformar_datos(df):
    ...
```

### Función para Guardar JSON en S3

```python
def guardar_json_s3(df, bucket, key):
    ...
```

### Función Principal

```python
def procesar_archivos():
    ...
```


## Manejo de Errores

El script incluye manejo de errores para situaciones comunes, como problemas de permisos de archivo y errores de credenciales de AWS.

## Contribuciones

Si deseas contribuir a este proyecto, por favor, haz un fork del repositorio y envía un pull request con tus mejoras.

## Licencia

Este proyecto está bajo la licencia MIT. Para más detalles, consulta el archivo LICENSE.
