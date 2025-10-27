import requests
from requests import HTTPError, ConnectionError

def send_alert(msg: str, bot_token: str, channel_id: str | int) -> None:
    
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
        'chat_id': channel_id,
        'text': msg
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except (HTTPError, ConnectionError) as e:
        print(f'ERROR: {e}')