from email import header
from wsgiref import headers
from pendulum import datetime
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
def Get_CIN_To_MasterData(passcin):
    # url = f"https://apisetu.gov.in/mca/v1/companies​/{passcin}"
    print(url)
    r = requests.get(f'https://apisetu.gov.in/mca/v1/companies/{passcin}',headers= headers)
    print(r)
    # print(r.json())

def Get_CIN_To_Director(passcin):
    passcin

Get_CIN_To_MasterData("U17120MH1990PTC055377")
