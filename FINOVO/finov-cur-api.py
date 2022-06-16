from cv2 import error
import requests
import json
import os
from os.path import exists
from datetime import date,datetime
import pandas as pd
from elasticsearch import Elasticsearch
import traceback
my_headers = {'x-api-key' : 'BOguclmwJ7','x-api-secret-key':'VGdbqAxigZzq59T9Mmtrg7yIK2cG6dv5t24LUYH5'}

out_list = []
today_date  = date.today()
dag_name = "finovo"
root = "/home/ubuntu/sanctions-scripts/FINOVO/"

db_name = "companies_data_finanvo"

check_list = []
failed_comp = []
error_c = 0




def Company_to_CIN(CompanyName):
    response = requests.get(f'https://api.finanvo.in/search/company?query={CompanyName}',headers=my_headers)
    data=response.json()
    print(data)
    return data['data']

def CIN_to_master_data(CIN):
    response = requests.get(f'https://api.finanvo.in/company/profile?CIN={CIN}')
    data=response.json()
    return data['data']

def CIN_to_Director(CIN):
    response = requests.get(f'https://api.finanvo.in/company/directors?CIN={CIN}')
    data=response.json()
    return data['data']

def CIN_to_Charges(CIN):
    response = requests.get(f'https://api.finanvo.in/company/charges?CIN={CIN}')
    data=response.json()
    return data['data']


def write_data_to_esdb(es,identity,article):
    global success_c
    global failure_c
    global failed_uids
    try:
        res = es.index(index=db_name,id=identity, body=article)
        success_c += 1
    except Exception as ex:
        failure_c += 1
        print(f"Data Inserting Error For UID : {identity}")
        failed_uids.append(identity)
        print(ex)
        print(f"--->>>{traceback.print_exc()}")


def get_data_from_excel(excelpath):
    global out_list,check_list,today_date,root,failed_comp
    df = pd.read_excel("/home/ubuntu/sanctions-scripts/FINOVO/MCA.xlsx")

    # es = Elasticsearch([{'host': '15.207.24.247', 'port': 9200}])

    for i,r in df.iterrows():
        pcin = r["CIN"]
        cname = r["Company Name"]

        print(f"{cname}  -- {pcin}")

        try:

            if pcin in check_list:
                continue
            else:
                check_list.append(pcin)
            comp_master = CIN_to_master_data(pcin)
            directors = CIN_to_Director(pcin)
            charges = CIN_to_Charges(pcin)
            comp_master["directors_data"] = directors
            comp_master["charges_data"] = charges
            # write_data_to_esdb(es,comp_master['CIN'],comp_master)
            out_list.append(comp_master)

            compnies = Company_to_CIN(cname)
            for compnay in compnies:
                if compnay["dataid"] in check_list:
                    continue
                else:
                    check_list.append(compnay["dataid"])
                comp_master = CIN_to_master_data(compnay["dataid"])
                directors = CIN_to_Director(compnay["dataid"])
                charges = CIN_to_Charges(compnay["dataid"])
                comp_master["directors_data"] = directors
                comp_master["charges_data"] = charges

                # write_data_to_esdb(es,comp_master['CIN'],comp_master)
                out_list.append(comp_master)
        except:
            print(f"error : {cname}")
            failed_comp.append(cname)
            error_c+=1

    print("*-*-*-*-*-*-*-*-*")
    print(f"Total Compnies scrapped : {len(out_list)}")
    print(f"Total failed : {error_c}")
    print("*-*-*-*-*-*-*-*-*")

def get_data_from_list(passlist):
    global out_list,check_list,today_date,root,failed_comp
    for r in passlist:
        cname = r

        print(f"{cname}")

        try:

            compnies = Company_to_CIN(cname)
            for compnay in compnies:
                if compnay["dataid"] in check_list:
                    continue
                else:
                    check_list.append(compnay["dataid"])
                comp_master = CIN_to_master_data(compnay["dataid"])
                directors = CIN_to_Director(compnay["dataid"])
                charges = CIN_to_Charges(compnay["dataid"])
                comp_master["directors_data"] = directors
                comp_master["charges_data"] = charges

                # write_data_to_esdb(es,comp_master['CIN'],comp_master)
                out_list.append(comp_master)
        except:
            print(f"error : {cname}")
            failed_comp.append(cname)
            error_c+=1

    print("*-*-*-*-*-*-*-*-*")
    print(f"Total Compnies scrapped : {len(out_list)}")
    print(f"Total failed : {error_c}")
    print("*-*-*-*-*-*-*-*-*")


# get_data_from_list(["a"])

# with open(f"{root}finvo-cur-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
#     json.dump(out_list, outfile, ensure_ascii=False, indent=4)

# with open(f"{root}finvo-cur-failed-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
#     json.dump(failed_comp, outfile, ensure_ascii=False, indent=4)




print(Company_to_CIN("a"))



