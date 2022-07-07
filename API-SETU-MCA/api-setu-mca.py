import requests
import json
from datetime import datetime,timedelta,date
import hashlib
import string
from string import punctuation

#API SETU ACCESS:
Clinet_ID  = "com.solyticspartners"
API_KEY = "6KklrKbP7QrbMFj728xOv7G1htqq8e9t"
Domain = "http//sams-solytics.com/"
App_ID = "886611"

url = "https://apisetu.gov.in/mca/v1"

headers = {
    'accept': 'application/json',
    'X-APISETU-APIKEY': '6KklrKbP7QrbMFj728xOv7G1htqq8e9t',
    'X-APISETU-CLIENTID': 'com.solyticspartners',
    }
def Get_MasterData_From_CIN(passcin):
    try:
        r = requests.get(f'https://apisetu.gov.in/mca/v1/companies/{passcin}',headers= headers)
        # print(r)
        # print(r.json())
        # print('----')
        return r.json()
    except:
        return {}

def Get_Director_From_CING(passcin):
    try:
        r = requests.get(f'https://apisetu.gov.in/mca/v1/companies/{passcin}/directors',headers= headers)
        # print(r)
        # print(r.url)
        # print(r.json())
        return r.json()
    except:
        return {}

Get_MasterData_From_CIN("U17120MH1990PTC055377")

