from bs4 import BeautifulSoup
from lxml import etree
import requests
import json
import time
from datetime import datetime,date,timedelta
import hashlib
import os
from os.path import exists
import boto3
from copy import deepcopy

#NOTE: Object for output json file
out_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "interpol"

# input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/INTERPOL-RED/"
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n):
    return hashlib.sha256(((n+"Prohibited Investment List").lower()).encode()).hexdigest()    


URL = "https://www.interpol.int/How-we-work/Notices/View-Red-Notices"

webpage = requests.get(URL)
soup = BeautifulSoup(webpage.content, "html.parser")
dom = etree.HTML(str(soup))
# print(dom.xpath('//*[@id="firstHeading"]')[0].text)
country_para = {}
for i in dom.xpath("//select[@id='nationality']/option"):
    if i.values():
        country_para[i.values()[0]] = i.text


def datascraper(passobj):
    ret_list = []
    for k in passobj["_embedded"]["notices"]:
        eid = k["entity_id"]
        # selflink = f"https://ws-public.interpol.int/notices/v1/red/{eid}"
        selflink = k["_links"]["self"]["href"]
        
        forename = k["forename"] if k["forename"] else ""
        name = k["name"] if k["name"] else ""
        name = (name + forename).strip()
        
        alias = []
        if forename and name:
            alias.append(forename+name)
            
        dob = [k["date_of_birth"]] if k["date_of_birth"] else []
        nationalities = k["nationalities"] if k["nationalities"] else []
        contry = []
        for c in nationalities:
            try:
                contry.append(country_para[c])
            except:
                print(f"conyry Error for :{c}")
                pass
                
        ret_list.append({
            "uid" : eid,
            "name" : name,
            "alias_name" : alias,
            "country" : contry,
            "individual_details":{
                "date_of_birth" : dob,
            },
        })

    return ret_list


def datascraper(passobj):
    ret_list = []
    for k in passobj["_embedded"]["notices"]:
        eid = k["entity_id"]
        # selflink = f"https://ws-public.interpol.int/notices/v1/red/{eid}"
        selflink = k["_links"]["self"]["href"]
        # print(selflink)
        time.sleep(0.5)
        sr = requests.get(selflink)
        sdata = json.loads(sr.text)

        forename = sdata["forename"] if sdata["forename"] else ""
        name = sdata["name"] if sdata["name"] else ""
        name = (name + forename).strip()
        
        alias = []
        if forename and name:
            alias.append(forename+name)
        
        gender = sdata["sex_id"] if sdata["sex_id"] else ""
        if gender == "M":
            gender = "male"
        elif gender == "F":
            gender = "female"
        else:
            gender = ""

        dob = [sdata["date_of_birth"]] if sdata["date_of_birth"] else []

        pob = sdata["place_of_birth"] if sdata["place_of_birth"] else ""

        nationalities = sdata["nationalities"] if sdata["nationalities"] else []
        contry = []
        for c in nationalities:
            try:
                contry.append(country_para[c])
            except:
                print(f"conyry Error for :{c}")
                pass
                
        ret_list.append({
            "uid" : eid,
            "name" : name,
            "alias_name" : alias,
            "country" : contry,
            "individual_details":{
                "date_of_birth" : dob,
                "place_of_birth":pob,
                "gender" : gender
            },
            "dob" : dob,
        })

        # ret_list.append(
        #     {
        #     'uid' :get_hash(eid+name),
        #     'name':name,
        #     'alias_name':alias, 
        #     'country': contry,
        #     'list_type':"Individual",
        #     'last_updated':last_updated_string,
        #     'list_id':"USA_E20294",    
        #     "individual_details":{
        #         "date_of_birth" : dob,
        #         "place_of_birth":pob,
        #         "gender" : gender
        #     },
        #     'sanction_details':{},    
        #     'nns_status':"False",
        #     'address':[],
        #     'comment':"",   
        #     'sanction_list':{
        #         "sl_authority":"Department of Justice, USA",
        #         "sl_url":"https://www.justice.gov/eoir/list-of-currently-disciplined-practitioners",
        #         "watch_list":"North America Watchlists",
        #         "sl_host_country":"USA",
        #         "sl_type": "Sanctions",
        #         "sl_source":"Department of Justice Executive Office For Immigration Review ‐ Disciplined Practitioners List, USA",
        #         "sl_description":"List of the Immigration Review's Attorney practitioners were expelled from practice by the Department of Justice, USA",
        #             }
        #         })
    return ret_list


mainc = 0
out_list = []
for rl in range(15,121):
    b_agr_url = f"https://ws-public.interpol.int/notices/v1/red?&ageMin={rl}&ageMax={rl}&resultPerPage=160"
    nr = requests.get(b_agr_url)
    print(nr)
    data = json.loads(nr.text)
    t_data = data["total"]
    print(f"{rl}-->{t_data}")
    mainc+=t_data
    datac = 0
    if t_data > 160:
        if datac >= t_data:
            break
        for k in country_para.keys():
            b_agrn_url = f"https://ws-public.interpol.int/notices/v1/red?&ageMin={rl}&ageMax={rl}&resultPerPage=160&nationality={k}"
            r =  requests.get(b_agrn_url)
            data = json.loads(r.text)
            if data["total"] > 160:
                print(f"{k}---->{data['total']}")
                for g in ["M","F","U"]:
                    b_u_url = b_agrn_url+"&sexId=" + g
                    rm =  requests.get(b_u_url)
                    mdata = json.loads(rm.text)
                    try:
                        out_list = out_list + datascraper(data)
                    except:
                        print(f"Error:{b_u_url}")
                    
            if data["total"]!=0:
                try:
                    out_list = out_list + datascraper(data)
                except:
                    print(f"Error:{b_agrn_url}")
                datac = datac+data["total"]
                # print(datac)
        print(f'->{datac}')
    else:
        try:
            out_list = out_list + datascraper(data)
        except Exception as e:
            print(f"Error:{b_agr_url}")
            print(f"Error:{e}")
            
with open('inter.json', "w",encoding='utf-8') as outfile:
    json.dump(out_list, outfile,ensure_ascii=False,indent=2)