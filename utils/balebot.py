import requests
from input_configs import *

def send_bale_message(msg , chatID):
    payload = {
        "chat_id": chatID,
        "text": msg
    }
    response = requests.post(BaleURL, json=payload)
    print(response.json())
