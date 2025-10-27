from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from outcoming_requests import get_user_subscriptions, get_user_requests, info_outcoming_requests
import os


try:
    access_token = Variable.get('VK_ACCESS_TOKEN')
    user_id = Variable.get('VK_USER_ID')
    bot_token = Variable.get('TG_BOT_TOKEN')
    channel_id = Variable.get('TG_CHANNEL_ID')
except Exception:
    access_token = os.environ.get('VK_ACCESS_TOKEN')
    user_id = os.environ.get('VK_USER_ID')
    bot_token = os.environ.get('TG_BOT_TOKEN')
    channel_id = os.environ.get('TG_CHANNEL_ID')

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
    }

def get_subscriptions(**context):
    '''Получаем информацию о подписках на публичные аккаунты и исходящих заявках в друзья'''
    subscriptons = get_user_subscriptions(access_token, user_id)
    out_request = get_user_requests(access_token)
    high_ego = set(out_request).difference(subscriptons)

    context['ti'].xcom_push(key='high_ego', value=high_ego)

def send_subscriptions_info(**context):
    '''Отправка в Telegram информации о пользователях, к которым висит исходящая заявка'''
    high_ego = context['ti'].xcom_pull(task_ids='get_subscriptions', key='high_ego')

    info_outcoming_requests(access_token=access_token, user_ids=high_ego, tg_bot_token=bot_token, tg_channel_id=channel_id)

with DAG(
    dag_id='vk_etl_subscriptions',
    default_args=default_args,
    schedule_interval='15 0 * * *',
    catchup=False,
    tags=['vk_api', 'etl', 'subs']
    ) as dag:

    get_subscriptions_task = PythonOperator(
        task_id='get_subscriptions',
        python_callable=get_subscriptions,
        provide_context=True,
    )

    send_subscriptions_info_task = PythonOperator(
        task_id='send_subscriptions_info',
        python_callable=send_subscriptions_info,
        provide_context=True,
    )

    get_subscriptions_task >> send_subscriptions_info_task