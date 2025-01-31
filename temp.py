import requests
from keys import coinstats_api

url = "https://openapiv1.coinstats.app/coins?symbol=BTC"

headers = {
    "accept": "application/json",
    "X-API-KEY": coinstats_api
}

response = requests.get(url, headers=headers)

print(response.text)