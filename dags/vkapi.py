import requests
from requests import RequestException
from enum import Enum

class VKMethodType(Enum):
    GET = 'get'
    POST = 'post'


class VKApiError(Exception):
    def __init__(self, error_code: int, error_msg: str, method: str = ''):
        self.error_code = error_code
        self.error_msg = error_msg
        self.method = method
        super().__init__(f'VKApiError: error_code - {error_code}, error_msg - {error_msg}, method - [{method}]')

class VKApi:

    base_url = 'https://api.vk.com/method/'
    requests_limit = 1000

    def __init__(self, access_token: str, version: str = '5.131'):
        self.access_token = access_token
        self.version = version

    def _determine_http_method(self, method:str) -> str:
        method_type = method.split('.')[-1].lower()
        if method_type.startswith('get'):
            return VKMethodType.GET.value
        elif method_type.startswith(('delete', 'remove', 'set', 'add', 'create')):
            return VKMethodType.POST.value
        else:
            return VKMethodType.GET.value
        
    def call_method(self, method, params=None) -> dict:
        if params is None:
            params = {}
        
        params.update({'access_token': self.access_token, 'v': self.version})
        
        http_method = self._determine_http_method(method)
        url = f'{self.base_url}{method}'

        try:
            if http_method == VKMethodType.GET.value:
                response = requests.get(url, params=params)
            else:
                response = requests.post(url, params=params)

            data = response.json()

            if 'error' in data:
                error = data['error']
                raise VKApiError(error_code=error.get('error_code', 0),
                                error_msg=error.get('error_msg', 'Unknown error'),
                                method=method)
            return data
        
        except RequestException as e:
            raise RequestException(f'Error calling {method}: {str(e)}') from e
    
    def get_user_friends_info(self, user_id: str | int, fields: str = 'sex,bdate,city,education,status,last_seen') -> dict:
        params = {'user_id': user_id, 'fields': fields}
        method = 'friends.get'
        return self.call_method(method, params)
    
    def get_user_info(self, user_ids: str | int, fields: str = 'counters,career,relation') -> dict:
        params = {'user_ids': user_ids, 'fields': fields}
        method = 'users.get'
        return self.call_method(method, params)
    
    def get_job_info(self, job_id: str | int) -> dict:
        params = {'group_id': job_id}
        method = 'groups.getById'
        return self.call_method(method, params)
    
    def delete_friend(self, user_id: str | int) -> dict:
        params = {'user_id': user_id}
        method = 'friends.delete'
        return self.call_method(method, params)
    
    def get_subscriptions(self, user_id: str | int) -> list[int]:
        params = {'user_id': user_id, 'extended': 0}
        method = 'users.getSubscriptions'
        return self.call_method(method, params)
    
    def get_outcoming_requests(self, count: int = 1000) -> list[int]:
        if count > self.requests_limit:
            raise ValueError(f'Максимальное количество заявок, которые можно получить не может превышать {self.requests_limit} штук.')
        params = {'count': count, 'out': 1}
        method = 'friends.getRequests'
        return self.call_method(method, params)