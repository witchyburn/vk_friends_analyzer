import requests

class VKApi:

    base_url = 'https://api.vk.com/method/'
    requests_limit = 1000

    def __init__(self, access_token):
        self.access_token = access_token
        self.version = '5.131'

    def call_method(self, method, params=None) -> dict:
        if params is None:
            params = {}
        
        params.update({'access_token': self.access_token, 'v': self.version})
        
        metod_type = method.split('.')[-1].lower()
        if metod_type.startswith('get'):
            response = requests.get(f'{self.base_url}{method}', params=params)
        elif metod_type.startswith('delete'):
            response = requests.post(f'{self.base_url}{method}', params=params)
        return response.json()
    
    def get_user_friends_info(self, user_id: str | int, fields: str = 'sex,bdate,city,education,status,last_seen') -> dict:
        params = {'user_id': user_id, 'fields': fields}
        method = 'friends.get'
        return self.call_method(method, params)
    
    def get_user_info(self, user_ids: str, fields: str = 'counters,career,relation') -> dict:
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