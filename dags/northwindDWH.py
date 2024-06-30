from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from scripts.Load_data import *
from scripts.Transform_data import *
import logging

default_args = {
    'owner': 'Mahmoud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'northwindDWH',
    default_args=default_args,
    description='E-E northwindDWH pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

def load_credentials():
    return {
        "host": "172.19.0.2",
        "db_name": 'northwind_dwh',
        "user": 'airflow',
        "password": 'airflow'
    }



def database_preparation():
    try:
        db_credentials = load_credentials()
        logging.info('Database preparation started')
        creat_tables(**db_credentials)
        logging.info('Database preparation done')
    except Exception as E:
        logging.error("Error in database preparation: %s", E, exc_info=True)
    


def data_delivery():
    try:
        db_credentials = load_credentials()
        logging.info('Data delivery started')
        load_DIM_Employee(**db_credentials)
        load_DIM_Products(**db_credentials)
        load_dimCustomers(**db_credentials)
        load_Factsales(**db_credentials)
        logging.info('Data delivery done')
    except Exception as e:
        logging.error("Error in data delivery: %s", e, exc_info=True)

database_preparation_task = PythonOperator(
    task_id='database_preparation',
    python_callable=database_preparation,
    dag=dag,
)



data_delivery_task = PythonOperator(
    task_id='data_delivery',
    python_callable=data_delivery,
    dag=dag,
)

database_preparation_task >>  data_delivery_task
