from datetime import datetime, timedelta

import re
import json
import backoff
import requests
import singer
from singer import metrics
from ratelimit import limits, sleep_and_retry
from requests.exceptions import ConnectionError

LOGGER = singer.get_logger()

class Server429Error(Exception):
    pass

class InvalidAuthException(Exception):
    pass

def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Playvox, triggering backoff: %d try",
                details.get("tries"))

class PlayvoxClient(object):
    URL_TEMPLATE = 'https://{}.cloud.agyletime.io/'

    def __init__(self, config, config_path):
        self.config = config
        self.__config_path = config_path
        self.__client_id = config.get('client_id')
        self.__client_secret = config.get('client_secret')
        self.__subdomain = config['subdomain']
        self.__session = requests.Session()
        self.__access_token = config.get('access_token')
        self.__expires_at = config.get('token_expiry')
        if self.__expires_at:
          self.__expires_at = datetime.strptime(self.__expires_at, "%Y-%m-%dT%H:%M:%S.%f")
        self.start_date = config.get('start_date')
        self.__client_url = self.URL_TEMPLATE.format(self.__subdomain)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    def refresh_access_token(self):

        # https://support.agyletime.com/hc/en-us/articles/4403341662105-Getting-Started-Authenticating-Example-Calls-with-Postman
        refresh_url = "https://{}.cloud.agyletime.io/api/authority/token".format(self.__subdomain)
        
        payload = {
            'grant_type': 'client_credentials'
        }
        
        response = self.__session.request(
            "POST",
            refresh_url,
            auth=(self.__client_id, self.__client_secret),
            data=payload)
    
        if response.status_code == 401:
            raise InvalidAuthException(response.text)
        self.__access_token = response.json()["data"]["accessToken"]

        self.__expires_at = datetime.utcnow() + \
            timedelta(seconds=response.json()["data"]["expiresIn"] - 10) # pad by 10 seconds for clock drift
        
        with open(self.__config_path) as file:
            config = json.load(file)
        
        config['access_token'] = response.json()["data"]["accessToken"]
        config['token_expiry'] = self.__expires_at.isoformat()

        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def check_and_renew_access_token(self):
        
        expired_token = self.__expires_at is None or self.__expires_at <= datetime.utcnow()
        
        if expired_token:
            self.refresh_access_token()

    # Limiting the calls to 100 for every 30 seconds to proactively avoid RateLimitException
    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_tries=8,
                          on_backoff=log_backoff_attempt,
                          factor=3)
    @sleep_and_retry
    @limits(calls=100, period=30)
    def request(self,
                method,
                path=None,
                url=None,
                **kwargs):
    
        self.check_and_renew_access_token()   
            
        if url is None and path:
            url = '{}{}'.format(self.__client_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            
            if(kwargs['endpoint'] == 'users'):
                #Update URL based on https://support.agyletime.com/hc/en-us/articles/360011488813-Organisation-Users-API
                url = re.sub(r"//.*?\.cloud", "//api-us.cloud", url)      
                  
            del kwargs['endpoint']
        else:
            endpoint = None
        
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)
        
        with metrics.http_request_timer(endpoint) as timer:
            try:
                if 'params' in kwargs:
                    response = self.__session.request(method, url, headers=kwargs['headers'], params=kwargs['params'])
                elif 'data' in kwargs:
                    response = self.__session.request(method, url, headers=kwargs['headers'], data= kwargs['data'])
                else:
                    response = self.__session.request(method, url, headers=kwargs['headers'])
                
            except Exception as e:
                LOGGER.info("PLAYVOX Client Exception:, %s", e)
            
            metrics_status_code = response.status_code

            timer.tags[metrics.Tag.http_status_code] = metrics_status_code
        
        if response.status_code == 403:
            raise InvalidAuthException(response.text)
            
        #based on the ratelimit 'limits' set, this exception should never occur
        if response.status_code == 429:
            LOGGER.warn('Rate limit hit - 429')
            raise Server429Error(response.text)

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)
    
    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
