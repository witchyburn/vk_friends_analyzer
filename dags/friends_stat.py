import pandas as pd
import numpy as np
import datetime
import math
from vkapi import VKApi, VKApiError
from os import environ
import logging

log_level_name = environ.get('LOG_LEVEL', 'INFO')

logging.basicConfig(
    level=getattr(logging, log_level_name.upper(), logging.DEBUG),
    format='%(asctime)s - %(levelname)s - %(name)s - %(filename)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')


# 1. Получаем общую информацию о всех друзьях с помощью friends.get
# отбираем кандидатов на удаление из друзей - забаненых или удаленных пользователей

def get_general_info(access_token: str, user_id: str) -> tuple[list]:

    vk = VKApi(access_token=access_token)
    try:
        res = vk.get_user_friends_info(user_id=user_id)
        result = res['response']['items']
    except VKApiError as e:
        logging.error(f'{e}')
    else:
        friends_general = []
        deletion_candidates = []

        for friend in result:
            data = {}
            data['user_id'] = friend['id']
            data['sex'] = friend['sex']
            data['first_name'] = friend['first_name']
            data['last_name'] = friend['last_name']

            if 'deactivated' in friend:
                data['deactivated'] = friend['deactivated']
                deletion_candidates.append(data)
            else: 
                data['birth_dt'] = friend.get('bdate', None)
                data['city'] = friend.get('city', {}).get('title', None)
                uni_str = friend.get('university_name', None)
                data['university'] = uni_str if uni_str != '' else None
                data['last_seen'] = friend.get('last_seen', {}).get('time', None)
                friends_general.append(data)

        logging.info(f'Получена общая информация о друзьях юзера id{user_id}. Число записей: {len(friends_general)} штук')
        logging.info(f'Деактивированных пользователей среди друзей: {len(deletion_candidates)} штук')
        
    return friends_general, deletion_candidates


# 2. Удаляем из друзей "удаленные" и/или "забаненные" аккаунты

def delete_deactivated_friends(access_token: str, candidates: list[int]) -> None:

    vk = VKApi(access_token=access_token)

    if candidates:
        for candidate in candidates:
            if candidate['deactivated'] in ['deleted']: # можно также удалить тех, кто 'banned'
                name = candidate['first_name']
                last_name = candidate['last_name']
                try:
                    resp = vk.delete_friend(user_id=candidate['id'])
                except VKApiError as e:
                    logging.error(f'При удалении пользователя {name} {last_name} произошла ошибка.\nerror_code: {e.error_code}\nerror_msg: {e.error_msg}')
                else:
                    result = resp['response']
                    if result.get('success') == 1:
                        logging.info(f'Пользователь {name} {last_name} успешно удален из друзей')
    else:
        logging.info(f'Удалено друзей: {len(candidates)}. Забаненные или удаленные пользователи отсутствуют.')



# 3. Получаем дополнительную информацию по каждому другу

def extract_user_info(data: dict) -> dict:
    career_obj = data.get('career', [{}])
    career_obj = [{}] if career_obj == [] else career_obj
    return {
        'user_id': data['id'],
        'relation': data.get('relation'),
        'job_id': career_obj[-1].get('group_id'),
        'friends': data['counters'].get('friends', 0),
        'followers': data['counters'].get('followers', 0),
        'audios': data['counters'].get('audios', 0),
        'videos': data['counters'].get('videos', 0)
    }

def get_user_info(user_id: str | int, vk: VKApi) -> dict | None:
    try:
        res = vk.get_user_info(user_ids=user_id)
        return res['response'][0]
    except VKApiError as e:
        logging.error(f'Ошибка для пользователя {user_id}: {e}')
        return None

def get_additional_info(access_token: str, user_ids: list[int]) -> list[dict]:

    vk = VKApi(access_token=access_token)

    results_add = list(map(lambda uid: get_user_info(uid, vk), user_ids))
    valid_results_add = filter(None, results_add)
    friends_add = [extract_user_info(result) for result in valid_results_add]   
    
    logging.info(f'Получена доп. информация по каждому другу. Число записей: {len(friends_add)} штук')
    
    return friends_add


# 4. Получаем названия сообществ - мест работы

def extract_job(job_id: str | int, vk: VKApi) -> tuple | None:
    try:
        res = vk.get_job_info(job_id=job_id)
        return (job_id, res['response'][0]['name'])
    except VKApiError as e:
        logging.error(f'Ошибка для job_id {job_id}: {e}')
        return None

def get_job_places(access_token: str, job_ids: list[int]) -> list[dict]:
    vk = VKApi(access_token=access_token)
    jobs = list(map(lambda jid: extract_job(jid, vk), job_ids))
    valid_jobs = filter(None, jobs)
    jobs_mapped = [{'job_id': t[0], 'job_place': t[1]} for t in valid_jobs]

    logging.info(f'Получена информация о месте работы друзей. Число записей: {len(jobs_mapped)} штук')

    return jobs_mapped
    

# 5. Объединяем основную информацию о пользователях с дополнительной информацией и сведениях о месте работы

def get_total_info(job_info: list[dict], gen_info: list[dict], add_info: list[dict]) -> pd.DataFrame:

    jobs = pd.DataFrame(job_info)
    general = pd.DataFrame(gen_info)
    general['last_seen'] = pd.to_datetime(general['last_seen'], unit='s')

    add = pd.DataFrame(add_info)
    add[['relation', 'job_id']] = add[['relation', 'job_id']].astype('Int64')
    add[['friends', 'followers', 'audios', 'videos']] = add[['friends', 'followers', 'audios', 'videos']].astype('Int64')

    df = pd.merge(left=general, right=add, how='left', on='user_id')
    df = df.merge(jobs, how='left', on='job_id')
    df.fillna(np.nan, inplace=True)
    df.fillna({'relation': 0}, inplace=True)
    df.drop(columns=['job_id'], inplace=True)

    return df


def process_total_df(df: pd.DataFrame) -> pd.DataFrame:

    # 6. Получаем полную дату рождения - для тех, кто указал день, месяц и год
    def process_bdate(val):
        lst = str(val).split('.')
        if len(lst) < 3:
            pass
        else:
            dt = datetime.datetime.strptime(str(val), '%d.%m.%Y')
            return dt

    df['birth_date'] = df['birth_dt'].apply(process_bdate)

    # 7. Вычисляем полное число лет
    def calculate_age(bdate):
        if pd.isna(bdate):
            pass
        else:
            tdelta = (pd.to_datetime(datetime.date.today()) - bdate).days
            years_old = math.floor(tdelta / 365.25)
            return years_old

    df['age'] = df['birth_date'].apply(calculate_age)
    df['age'] = df['age'].astype('Int64')


    # 8. Получаем месяц рождения - для тех, кто указал день и месяц

    def extract_month(val):
        str_val = str(val)
        if pd.isna(val):
            pass
        else:
            lst = str_val.split('.')
            month_number = int(lst[1])
            month_name = datetime.date(2025, month_number, 1).strftime('%B')
            return month_name

    df['birth_month'] = df['birth_dt'].apply(extract_month)
    df.drop(columns=['birth_dt'], inplace=True)


    # 9. Маппим пол и семейное положение

    df['sex'] = df['sex'].apply(lambda x: 'f' if x == 1 else 'm')

    def relation_mapping(value):
        rel_map = {1: 'не женат/не замужем',
                2: 'есть друг/есть подруга',
                3: 'помолвлен/помолвлена',
                4: 'женат/замужем',
                5: 'всё сложно',
                6: 'в активном поиске',
                7: 'влюблён/влюблена',
                8: 'в гражданском браке',
                0: 'не указано'
            }

        relation = rel_map[value]
        return relation

    df['relation'] = df['relation'].apply(relation_mapping)

    logging.info(f'Сформирован датафрейм с полной информацией о друзьях. Rows: {df.shape[0]}, Columns: {df.shape[1]}')
    return df


# 10. Определяем пользователей, не заходивших в ВК более года назад

def show_long_ago(df: pd.DataFrame) -> None:

    year_ago = datetime.date.today() - datetime.timedelta(weeks=52)
    long_ago = df[df['last_seen'] < pd.to_datetime(year_ago)]

    first_names = long_ago['first_name'].to_list()
    last_names = long_ago['last_name'].to_list()
    ids = long_ago['user_id'].to_list()

    lost_list = list(zip(first_names, last_names, ids))

    logging.info(f'Пользователи, заходившие в ВК более года назад. Возможно, их нужно удалить из друзей.\n{lost_list}')

