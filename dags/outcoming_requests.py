from vkapi import VKApi
from telegram import send_alert
from os import environ
import datetime
import logging

log_level_name = environ.get('LOG_LEVEL', 'INFO')

logging.basicConfig(
    level=getattr(logging, log_level_name.upper(), logging.DEBUG),
    format='%(asctime)s - %(levelname)s - %(name)s - %(filename)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')


# 1. Получение идентификаторов публичных пользователей, входящих в состав подписок пользователя

def get_user_subscriptions(access_token: str, user_id: str) -> list[int]:
    vk = VKApi(access_token=access_token)
    resp = vk.get_subscriptions(user_id=user_id)
    result = resp['response']['users']['items']
    logging.info(f'Количество публичных аккаунтов, на которые у меня есть подписка: {len(result)}.')
    return result


# 2. Получение информации об отправленных заявках на добавление в друзья для текущего пользователя

def get_user_requests(access_token: str) -> list[int]:
    vk = VKApi(access_token=access_token)
    resp = vk.get_outcoming_requests()
    result = resp['response']['items']
    return result


# 3. Мониторинг появления исходящих заявок в друзья к пользователям -
#    либо удалившие меня из друзей, либо пока не принявшие исходящую от меня заявку в друзья.
#    Публичные пользователи, чей профиль я сознательно подписалась читать, здесь не учитываются.
#    Информация о том, кому отправлена исходящая заявка, отправляется в Telegram.

def info_outcoming_requests(access_token: str,
                            user_ids: set | list,
                            tg_bot_token: str,
                            tg_channel_id: str | int) -> None:
    
    platforms = {
        1: 'мобильная версия',
        2: 'приложение для iPhone',
        3: 'приложение для iPad',
        4: 'приложение для Android',
        5: 'приложение для Windows Phone',
        6: 'приложение для Windows 10',
        7: 'полная версия сайта'
    }

    if user_ids:
        logging.info(f'Количество исходящих заявок на добавление в друзья: {len(user_ids)}')
        vk = VKApi(access_token=access_token)
        for user in user_ids:
            info = vk.get_user_info(user_ids=user, fields='last_seen,last_name_gen,first_name_gen')['response'][0]
            
            name, last_name = info['first_name_gen'], info['last_name_gen']
            platform, time = info['last_seen']['platform'], info['last_seen']['time']
            time = datetime.datetime.fromtimestamp(time)

            msg = f'Либо вы ожидаете одобрения исходящей заявки от {name} {last_name}, либо данный пользователь удалил(а) Вас из друзей.\n\
Последний визит {name} - {time}, с платформы - {platforms[platform]}.'
            logging.info(msg)
            send_alert(msg, tg_bot_token, tg_channel_id)

    else:
        logging.info('Исходящие заявки на добавление в друзья отсутствуют.')