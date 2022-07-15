import requests
import json
from scrapy.http import HtmlResponse
import hashlib
from datetime import datetime,timedelta,date
from deep_translator import GoogleTranslator
import boto3
import os
from os.path import exists


#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "sng-780"
root = "/home/ubuntu/sanctions-scripts/SNG-780/"

# input_filename  = f'{dag_name}-inout.pdf'
output_filename = f'{dag_name}-output-{today_date}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date}.json'
removed_filename = f'{dag_name}-removed-{today_date}.json'
old_output_filename = f'{dag_name}-output-{yesterday}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
# root = ""
# ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


def get_hash(n):
    return hashlib.sha256(((n+"Ukraine Ministry of Internal Affairs Most Wanted List, Ukraine (European Watchlists UKR_E20266)").lower()).encode()).hexdigest()
 

def langtranslation(to_translate):
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
    except:
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
        except:
            print(f">>>Translartion Bug : {to_translate}")
            translated = to_translate   
    return translated

# -*- coding: utf-8 -*-
def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def alias_maker(n):
    na = n.split(" ")
    alis = []
    if len(na)==2:
        alis.append((na[1].strip()+" "+na[0].strip()).strip())
    elif len(na) == 3:
        alis.append((na[2].strip()+" "+na[0].strip() + " "+na[1].strip()).strip())
    return alis

def process_data():
    global out_list,last_updated_string,total_profile_available
    p = 4781
    while True:
        print(f"->{p}")
        url = f"https://wanted.mvs.gov.ua/searchperson?page={p}"
        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.33","cookie": "ASP.NET_SessionId=h2lhrhetjs2tghlg0wh2vgra; SC_ANALYTICS_GLOBAL_COOKIE=ee5a871126d5485a822eac3007f50b7b|False; MAScookie=!dIhRyMFhuUiVpiKMUcAcxB4uQg+8eQJK36TNFZZkDdG1F5VpHxDfW+o5TAGIc1M9z2feFILOTq7Ztt779seIU2ZCcoI0WQu9NUDZeu1vwxuWonZFSiLq0w+Ha8Gdx10KZRdSjsy1jb4p5AUFsS8Q2F9NAKeiikA=; TS01dd65cc=01df21a10a922522902721bc0e53dd34caf6a0df1b727d55eaadc2ab8e038e4bab74abae023af7c4ca89e7b372c62c67c30bf8846635925a6e0b8900f42b40dbf3da5152cce0be78ef88ced5efc72058a5df5384c0b40ae00dd81c4f7b99ae5e6ff89679e3; _ga=GA1.3.1251844780.1654762359; _gid=GA1.3.1438920284.1654762359; _hjMinimizedPolls=488055; _sp_id.a65f=c1ac2f71-dca1-4ccd-a7d1-e0fa0abced7a.1654762360.1.1654762396.1654762360.82ce2841-66c1-4c27-adbe-0b9fdf927f2e; _hjSessionUser_756852=eyJpZCI6ImRhNzZlOGEyLWVkNTEtNThhZC1hMTI1LWVhOWFmNDZhNDczYyIsImNyZWF0ZWQiOjE2NTQ3NjIzNjAzNjEsImV4aXN0aW5nIjp0cnVlfQ=="}
        res = requests.get(url, headers= headers)
        resp = HtmlResponse("example.com",body=res.text,encoding='utf-8')
        urls = resp.xpath("//div[@class='section-content']//div/a/@href").getall()

        for i in urls:
            ur = "https://wanted.mvs.gov.ua" + i
            ir = requests.get(ur, headers=headers)
            iresp = HtmlResponse("example.com",body=ir.text,encoding='utf-8')
            surname = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[5]/text()").get("").strip()
            name = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[6]/text()").get("").strip()
            midname = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[7]/text()").get("").strip()
            rsurname = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[8]/text()").get("").strip()
            rname = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[9]/text()").get("").strip()
            rmidname = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[10]/text()").get("").strip()
            dob = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[11]/text()").get("").strip()
            gender = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[12]/text()").get("").strip()
            # article = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[13]/text()").get("").strip()
            # charges = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[14]/text()").get("").strip()
            # contact = iresp.xpath("(//div[@class='info-list']//div[@data='second'])[15]/text()").get("").strip()

            if name and midname and surname:
                fname = f'{name} {midname} {surname}'
                rname = f'{rname} {rmidname} {rsurname}'
            elif name and surname:
                fname = f'{name} {surname}'
                rname = f'{rname} {rsurname}'
            elif name and midname:
                fname = f'{name} {midname}'
                rname = f'{rname} {rmidname}'
            elif name:
                fname = f'{name}'
                rname = f'{rname}'
            elif midname:
                fname = f'{midname}'
                rname = f'{rmidname}'
            elif surname:
                fname = f'{surname}'
                rname = f'{surname}'
            else:
                continue
            
            print(fname)
            alis = [fname,rname]
            fname =  langtranslation(fname)
            alis.extend(alias_maker(fname))

            if dob:
                dob = [dob.replace('.','/')]
            else:
                dob = []
            
            if gender:
                if gender=="чоловіча":
                    gender="male"
                elif gender=="жіноча":
                    gender="female"
                else:
                    gender=""

            out_list.append(
                    {
                    "uid": get_hash(fname),
                    "name": fname,
                    "alias_name": alis,
                    "list_type": "Individual",
                    "individual_details": {
                        "date_of_birth" : dob,
                        "gender" : gender
                    },
                    "last_updated": last_updated_string,
                    "nns_status": "False",
                    "sanction_details": {},
                    "documents": {},
                    "country": [
                        "Ukraine"
                    ],
                    "address": [
                        {
                            "complete_addrss": "",
                            "country": ""
                        }
                    ],
                    "comment": "",
                    "sanction_list": {
                        "sl_authority": "Ukraine Ministry of Internal Affairs, Ukraine",
                        "sl_url": "https://wanted.mvs.gov.ua/searchperson",
                        "sl_host_country": "Ukraine",
                        "sl_type": "Sanctions",
                        "watch_list": "European Watchlists",
                        "sl_source": "Ukraine Ministry of Internal Affairs Most Wanted List, Ukraine",
                        "sl_description": "List of individuals wanted as criminals by Ukraine Ministry of Internal Affairs, Ukraine",
                        "list_id": "UKR_E20266"
                    }
                }
            )

            # if article:
            #     article = langtranslation(article)
            # if charges:
            #     charges = langtranslation(charges)


        next_c = resp.xpath("//li[last()][@class='page-item disabled']").get()
        if next_c:
            break
        p+=1
    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4,default=str)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4,default=str)

def CompareDocument():
    try:
        with open(f'{op_path}/{old_output_filename}', 'rb') as f:
            data = f.read()
    except:
        print("---------------------Alert--------------------------")
        print(f"There is not any old file for date: {yesterday.ctime()}")
        print("----------------------------------------------------")
        data = "No DATA"
    old_list = []
    if data != "No DATA":   
        old_list = json.loads(data)

    with open(f'{op_path}/{output_filename}', 'rb') as f:
        new_data = f.read()
    new_list =  json.loads(new_data)

    new_profiles = []
    removed_profiles = []
    updated_profiles = []

    old_dict = {}
    if old_list:
        for val in old_list:
            old_dict[val["uid"]] = val
        
    new_uid_list = []
    for val1 in new_list:
        new_uid = val1["uid"]
        new_uid_list.append(new_uid)
        if new_uid in old_dict.keys():
            # print("Already in List")
            for i in val1:
                if i!= "last_updated":
                    try:
                        if val1[i] != old_dict[val1["uid"]][i]:
                            # print(f"Updataion Detected on: {val1['uid']} for: {i}")
                            # print(f"Updataion Detected for: {val1[i]}")
                            updated_profiles.append(val1)
                            break
                    except:
                        # print(f"Updataion Detected on: {val1['uid']} for: {i}")
                        # print(f"Updataion Detected for: {val1[i]}")
                        updated_profiles.append(val1)
                        break

        else:
            new_profiles.append(val1)
            # print(f"New Profile Detected : {val1['uid']}")
    

    for val2 in old_dict.keys():
        if val2 not in new_uid_list:
            # print(f"Removed Profile Detected : {val2}")
            removed_profiles.append(old_dict[val2])

    print("------------------------LOG-DATA---------------------------")
    print(f"total New Profiles Detected:     {len(new_profiles)}")
    print(f"total Updated Profiles Detected: {len(updated_profiles)}")
    print(f"total Removed Profiles Detected: {len(removed_profiles)}")
    print("-----------------------------------------------------------")

    if len(new_list)==0:
        removed_profiles = []
        raise ValueError("Error : Data Parsing Error.... fix it quick ⚒️")
    try:
        with open(f'{dp_path}/{diffrance_filename}', "w",encoding='utf-8') as outfile:
            json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(dp_path)
        with open(f'{dp_path}/{diffrance_filename}', "w",encoding='utf-8') as outfile:
            json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

    try:
        with open(f'{rm_path}/{removed_filename}', "w",encoding='utf-8') as outfile:
            json.dump(removed_profiles, outfile,ensure_ascii=False)
    except FileNotFoundError:
        os.mkdir(rm_path)
        with open(f'{rm_path}/{removed_filename}', "w",encoding='utf-8') as outfile:
            json.dump(removed_profiles, outfile,ensure_ascii=False)

    if exists(lp_path):
        with open(f'{lp_path}',"a") as outfile:
            passing = f"{last_updated_string},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        # s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
        s3.upload_file(f'{op_path}/{output_filename}',"sams-scrapping-data",f"{dag_name}/parced/{output_filename}")
        s3.upload_file(f'{dp_path}/{diffrance_filename}',"sams-scrapping-data",f"{dag_name}/diffrance/{diffrance_filename}")
        s3.upload_file(f'{rm_path}/{removed_filename}',"sams-scrapping-data",f"{dag_name}/removed/{removed_filename}")
        s3.upload_file(f'{lp_path}',"sams-scrapping-data",f"{dag_name}/{lp_name}")
        print("uploaded files to s3")      
    except Exception as e:
        print("------------------🔴ALERT🔴------------------------")
        print("Can not upload files to s3")
        print("Exception : " , e)
        print("----------------------------------------------------")

process_data()
# CompareDocument()
# UploadfilestTos3() 