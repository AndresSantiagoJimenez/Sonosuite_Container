from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

# Ruta al archivo Python que contiene el código a ejecutar
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')
script_path = os.path.join(src_dir, 'Codigo_ftp_Amazon.py')

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir la función que ejecutará el código Python
def run_python_script():
    exec(open(script_path).read())

# Crear el DAG
with DAG('procesamiento_datos_amazon',
         default_args=default_args,
         description='DAG para procesamiento de datos desde Amazon FTP',
         schedule_interval=timedelta(days=1),  # Ejecutar diariamente
         catchup=False) as dag:

    # Definir el operador que ejecutará el script Python
    run_script_task = PythonOperator(
        task_id='ejecutar_script_ftp_amazon',
        python_callable=run_python_script,
    )

# Definir el orden de ejecución de los tasks (en este caso, solo uno)
run_script_task
