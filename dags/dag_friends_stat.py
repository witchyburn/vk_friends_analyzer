from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from sqlalchemy.types import VARCHAR
from friends_stat import get_general_info, delete_deactivated_friends, get_additional_info, get_job_places, get_total_info, process_total_df, show_long_ago
from datetime import datetime, timedelta
import os
import tempfile
import pandas as pd
import logging

try:
    access_token = Variable.get('VK_ACCESS_TOKEN')
    user_id = Variable.get('VK_USER_ID')
except Exception:
    access_token = os.environ.get('VK_ACCESS_TOKEN')
    user_id = os.environ.get('VK_USER_ID')


default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=4)
    }

def extract_general_info(**context):
    '''Получаем общую информацию о друзьях и отбираем забаненых или удаленных друзей'''

    friends_general, deletion_candidates = get_general_info(access_token, user_id)

    context['ti'].xcom_push(key='friends_general', value=friends_general)
    context['ti'].xcom_push(key='deletion_candidates', value=deletion_candidates)

def clean_deactivated_friends(**context):
    '''Убираем из друзей "удаленные" и/или "забаненные" аккаунты'''

    deletion_candidates = context['ti'].xcom_pull(task_ids='extract_general_info', key='deletion_candidates')
    delete_deactivated_friends(access_token, deletion_candidates)

def extract_additional_info(**context):
    '''Получаем дополнительную информацию о друзьях'''
    friends_general = context['ti'].xcom_pull(task_ids='extract_general_info', key='friends_general')
    friends_ids = [data['user_id'] for data in friends_general]
    
    friends_add = get_additional_info(access_token, friends_ids)
    
    # Сохраняем доп. информацию и извлекаем ID работ
    job_ids = [data['job_id'] for data in friends_add if data['job_id'] is not None]
    
    context['ti'].xcom_push(key='friends_add', value=friends_add)
    context['ti'].xcom_push(key='job_ids', value=job_ids)
    
def extract_job_info(**context):
    '''Получаем места работы рузей (названия сообществ)'''
    job_ids = context['ti'].xcom_pull(task_ids='extract_additional_info', key='job_ids')
    
    jobs_mapped = get_job_places(access_token, job_ids)
    context['ti'].xcom_push(key='jobs_mapped', value=jobs_mapped)

def transform_data(**context):
    '''Объединение и трансформация данных'''
    
    jobs_mapped = context['ti'].xcom_pull(task_ids='extract_job_info', key='jobs_mapped')
    friends_general = context['ti'].xcom_pull(task_ids='extract_general_info', key='friends_general')
    friends_add = context['ti'].xcom_pull(task_ids='extract_additional_info', key='friends_add')
    
    # Объединяем данные
    df = get_total_info(jobs_mapped, friends_general, friends_add)
    df = process_total_df(df)
    
    # Анализ неактивных пользователей
    show_long_ago(df)
    
    # Сохраняем готовый DataFrame
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.parguet', delete=False) as f:
        df.to_parquet(f.name)
        temp_path = f.name
        
    context['ti'].xcom_push(key='temp_df_path', value=temp_path)

def load_data(**context):
    '''Загрузка данных в БД'''

    temp_path = context['ti'].xcom_pull(task_ids='transform_data', key='temp_df_path')
    
    if not temp_path or not os.path.exists(temp_path):
        logging.error("Временный файл не найден")
        return
    
    # Восстанавливаем DataFrame
    df = pd.read_parquet(temp_path)

    # Загружаем в БД
    hook = PostgresHook(postgres_conn_id='postgres_default')
    with hook.get_sqlalchemy_engine().connect() as conn:
        try:
            df.to_sql(
                name='statistics',
                con=conn,
                if_exists='append',
                index=False,
                method='multi',
                dtype={
                    'first_name': VARCHAR(50),
                    'last_name': VARCHAR(50),
                    'city': VARCHAR(30),
                    'university': VARCHAR(100),    
                    'relation': VARCHAR(50),
                    'job_place': VARCHAR(100)
                }
            )
            logging.info(f'Успешно загружено {len(df)} записей в БД')
            
        except Exception as e:

            logging.error(f'Ошибка при загрузке в БД: {e}')

    if os.path.exists(temp_path):
        os.unlink(temp_path)
        logging.info(f"Временный файл удален: {temp_path}")

with DAG(
    dag_id='vk_etl_friends',
    default_args=default_args,
    schedule_interval='10 0 * * *',
    catchup=False,
    tags=['vk_api', 'etl', 'friends']
    ) as dag:

    extract_general_task = PythonOperator(
        task_id='extract_general_info',
        python_callable=extract_general_info,
        provide_context=True,
    )

    clean_friends_task = PythonOperator(
        task_id='clean_deactivated_friends',
        python_callable=clean_deactivated_friends,
        provide_context=True,
    )

    extract_additional_task = PythonOperator(
        task_id='extract_additional_info',
        python_callable=extract_additional_info,
        provide_context=True,
    )

    extract_jobs_task = PythonOperator(
        task_id='extract_job_info',
        python_callable=extract_job_info,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract_general_task >> clean_friends_task
    extract_general_task >> extract_additional_task
    extract_additional_task >> extract_jobs_task
    extract_jobs_task >> transform_task
    transform_task >> load_task