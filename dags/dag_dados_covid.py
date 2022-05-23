import glob
import os

from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
import json
import pandas as pd

api_credentials = Variable.get("api_credentials", deserialize_json=True)
path = Variable.get("path_vaccinated")
compression = Variable.get("compression_vaccinated")
filename = Variable.get("filename_vaccinated")
blob_name = Variable.get("storage_blob_name")
default_args = {
    'owner': 'SergioPassos',
    'depends_on_past': False,
    'retries': 1
}


def processing_vaccinated(ti, path, filename, compression):
    vaccinated = ti.xcom_pull(task_ids=['get_vaccinated'])
    pd.set_option('display.max_columns', None)
    print(vaccinated[0]['hits']['hits'])
    if not len(vaccinated) or 'hits' not in vaccinated[0]:
        raise ValueError('Vaccinated is empty')

    df = pd.json_normalize(vaccinated[0]['hits']['hits'])
    df.info()

    df2 = pd.DataFrame(df, dtype=str)
    df2.info()
    print("df2.head: )", df2.head())
    df2.to_parquet(path=path + filename, compression=compression, index=False)


def local_to_adls(dir_target, ds, filepath):
    adls = WasbHook(wasb_conn_id='conn_data_lake_raw')
    if glob.glob(filepath):
        for f in glob.glob(filepath):
            print("File to move {}".format(f))
            blob_path = dir_target + ds + '/' + f.split('/')[-1]
            print(blob_path)
            adls.delete_file(container_name='raw', blob_name=blob_path, ignore_if_missing=True)
            adls.load_file(file_path=f, container_name='raw', blob_name=blob_path)
    else:
        raise ValueError('Directory is empty no file to copy')


def remove_local_file(filepath):
    files = glob.glob(filepath)
    for f in files:
        os.remove(f)


with DAG('dag_dados_covid',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False,
         start_date=datetime.now()) as dag:
    api_availabe = HttpSensor(
        task_id='api_avaliable',
        http_conn_id='datasus_api',
        headers=api_credentials,
        endpoint='/_search'
    )
    get_vaccinated = SimpleHttpOperator(
        task_id='get_vaccinated',
        http_conn_id='datasus_api',
        endpoint='/_search',
        data=json.dumps({"size": 100}),
        method='POST',
        headers=api_credentials,
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    processing_vaccinated = PythonOperator(
        task_id='processing_vaccinated',
        python_callable=processing_vaccinated,
        op_kwargs={'path': path, 'filename': filename, 'compression': compression}
    )
    upload_to_adls = PythonOperator(
        task_id='upload_to_adls',
        python_callable=local_to_adls,
        op_kwargs={
            'dir_target': 'vaccinated/',
            'filepath': './data/sus/covid/vaccinated/*.parquet'
        }
    )
    remove_local_file = PythonOperator(
        task_id='remove_local_file',
        python_callable=remove_local_file,
        op_kwargs={
            'filepath': '.	/data/sus/covid/vaccinated/*.parquet'
        }
    )
    api_availabe >> get_vaccinated >> processing_vaccinated >> upload_to_adls >> remove_local_file
