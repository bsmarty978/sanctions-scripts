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
    return hashlib.sha256(((n+"Interpol Most Wanted Red Notices List" + "INT_T30057").lower()).encode()).hexdigest()    


URL = "https://www.interpol.int/How-we-work/Notices/View-Red-Notices"

webpage = requests.get(URL)
soup = BeautifulSoup(webpage.content, "html.parser")
dom = etree.HTML(str(soup))
# print(dom.xpath('//*[@id="firstHeading"]')[0].text)
country_para = {}
for i in dom.xpath("//select[@id='nationality']/option"):
    if i.values():
        country_para[i.values()[0]] = i.text


def datascraper1(passobj):
    ret_list = []
    for k in passobj["_embedded"]["notices"]:
        eid = k["entity_id"]
        # selflink = f"https://ws-public.interpol.int/notices/v1/red/{eid}"
        selflink = k["_links"]["self"]["href"]
        
        forename = k["forename"] if k["forename"] else ""
        name = k["name"] if k["name"] else ""
        name = (name + " " +forename).strip()
        
        alias = []
        if forename and name:
            alias.append(forename+ " " +name)
            
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
        time.sleep(0.2)
        sr = requests.get(selflink)
        sdata = json.loads(sr.text)

        forename = sdata["forename"] if sdata["forename"] else ""
        name = sdata["name"] if sdata["name"] else ""
        pass_name = (forename + " " + name).strip()
        
        alias = []
        if forename and name:
            alias.append(name+ " " +forename)
        
        gender = sdata["sex_id"] if sdata["sex_id"] else ""
        if gender == "M":
            gender = "male"
        elif gender == "F":
            gender = "female"
        else:
            gender = ""

        dob = [sdata["date_of_birth"]] if sdata["date_of_birth"] else []

        pob = sdata["place_of_birth"] if sdata["place_of_birth"] else ""
        adrs = []
        if pob:
            adrs.append({
                "complete_address" : pob,
                "country" : ""
            })

        nationalities = sdata["nationalities"] if sdata["nationalities"] else []
        contry = []
        for c in nationalities:
            try:
                contry.append(country_para[c])
            except:
                print(f"conyry Error for :{c}")
                pass
                
        # ret_list.append({
        #     "uid" : eid,
        #     "name" : name,
        #     "alias_name" : alias,
        #     "country" : contry,
        #     "individual_details":{
        #         "date_of_birth" : dob,
        #         "place_of_birth":pob,
        #         "gender" : gender
        #     },
        #     "dob" : dob,
        # })

        ret_list.append(
            {
            'uid' :get_hash(eid+pass_name),
            'name':pass_name,
            'alias_name':alias, 
            'country': contry,
            'list_type':"Individual",
            'last_updated':last_updated_string,
            'list_id':"INT_T30057",    
            "individual_details":{
                "date_of_birth" : dob,
                "place_of_birth":pob,
                "gender" : gender
            },
            'sanction_details':{},    
            'nns_status':"False",
            'address':adrs,
            'comment':"",   
            "sanction_list" : {
            "sl_authority" : "Interpol",
            "sl_url" : "https://www.interpol.int/en/How-we-work/Notices/View-Red-Notices#",
            "watch_list" : "Global Watchlists",
            "sl_host_country" : "United Nations",
            "sl_type" : "Interpol Wanted",
            "sl_source" : "Interpol Most Wanted Red Notices List",
            "sl_description" : "Red Notices are issued for fugitives wanted either for prosecution or to serve a sentence. A Red Notice is a request to law enforcement worldwide to locate and provisionally arrest a person pending extradition, surrender, or similar legal action by Interpol."
            },
                })
    return ret_list


def process_data():
    global out_list,last_updated_string,total_profile_available
    mainc = 0
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
                
    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)

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
            passing = f"{last_updated_string},,{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
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
CompareDocument()
UploadfilestTos3() 