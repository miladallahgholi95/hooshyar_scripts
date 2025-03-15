import requests
from input_configs import *

def send_bale_message(msg):
    payload = {
        "chat_id": CHAT_ID,
        "text": msg
    }
    response = requests.post(BaleURL, json=payload)
    print(response.json())
