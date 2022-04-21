from bs4 import BeautifulSoup as bs
import pandas as pd
pd.set_option('display.max_colwidth', 500)
import requests
from datetime import datetime,date,timedelta
import json
import hashlib
import os
from os.path import exists
from copy import deepcopy
import boto3

#NOTE: Object for output json file
out_list = []
input_list = []
total_profile_available = 0

#NOTE: Filename according to the date :
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "nia-india"

input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
root = "/home/ubuntu/sanctions-scripts/NIA-IN/"
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"


cwd=os.getcwd()
print("Current working directory: ", cwd)

def alias_names(name):
    alias_list=[]
    subname = name.split(' ')
    l = len(subname)
    if l>=3:
        name1 = subname[l-1] + " " + subname[0]
        name2 = subname[l-2] + " " + subname[0]
        alias_list.append(name1)
        alias_list.append(name2)
    if l==2:
        name1 = subname[1] + " " + subname[0]
        alias_list.append(name1)
    
    return alias_list

def process_data():

    page= requests.get("https://www.nia.gov.in/most-wanted.htm", verify=False)
    soup=bs(page.content,"lxml")

    data_json = []
    print('******** Fetching The Data! ********')
    for i in soup.find_all('a', href=True):
        if '.htm?' in i['href']:
            try:
                i_data = {}
                d_page= requests.get("https://www.nia.gov.in/"+i['href'], verify=False)
                # print("Fetching from " + "https://www.nia.gov.in/"+i['href'])
                d_soup=bs(d_page.content,"lxml")
                i_data["name"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblName")][0]
                i_data["address"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblAddress")][0]
                i_data["aliases"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblAliases")][0]
                i_data["parentage"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblParentage")][0]
                i_data["accused_status"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblAccusedStatus")][0]
                i_data["age_or_dob"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblAge")][0]
                i_data["organisation"]=[i.text.replace(u'\xa0',' ').replace(u'\n\t\t\t',' ').replace(u'\n',',').strip() for i in d_soup.find_all(id="ContentPlaceHolder1_WantedDetail1_lblOrganization")][0]
                i_data["link"] = "https://www.nia.gov.in/" + i['href']
                data_json.append(i_data)
                # print(i_data)
                print('==============')
            except:
                continue
    print('******** Fetching Complete! ********')
    # ### Now formatting the data
    data = data_json
    for i in data:
        i['aliases'] = i['aliases'].split('@')
        i['all_aliases'] = []
        i['all_aliases'] += (alias_names(i['name']))
        for j in i['aliases']:
            i['all_aliases'] += (alias_names(j))
        i['all_aliases'] = list(set(i['all_aliases']))
        
        #Adding country
        if (i['address'].find('Pakistan') == -1):
            i['country'] = 'India'
        else:
            i['country'] = 'Pakistan'
            
        #Adding gender
        if i['parentage']:
            if i['parentage'][0].lower() == 'd' or i['parentage'][0].lower() == 'w':
                i['gender'] = 'Female'
            else:
                i['gender'] = 'Male'
        else:
            i['gender'] = ''
        # removing S/O, D/O, W/O and R/O
        if i['address']:
            try:
                if i['address'][1]=='/':
                    i['address'] = i['address'][4:]
            except:
                i['address'] = i['address']
        if i['parentage']:
            try:
                if i['parentage'][1]=='/':
                    i['parentage'] = i['parentage'][4:]
            except:
                i['parentage'] = i['parentage']


    final_list1 = []
    for i in data:
        final_list1.append(
            {
                "uid": hashlib.sha256(((i['name'] + i['address'] + "National Investigation Agency Most Wanted").lower()).encode()).hexdigest(),
                "list_type" : "individual",
                "name": i['name'],
                "alias_name": i['all_aliases'],
                "gender": i['gender'],
                "date_of_birth": [],
                "country": i['country'],
                "family-tree": {
                    "Father": i['parentage']
                },
                "list_id": "IND_T30005",
                "designation": "",
                "last_updated": last_updated_string,
                "address": [
                                {
                                    "complete_address": i['address'],
                                    "state": "",
                                    "city": "",
                                    "country": i['country']
                                }
                ],
                "nns_status": "False",
                "organisation": i['organisation'],
                "documents": {},
                "comment": "Accused of " + i['accused_status'],
                "sanction_list" : {
                    "sl_authority" : "National Investigation Agency",
                    "sl_url" : "https://www.nia.gov.in/most-wanted.htm",
                    "watch_list" : "Indian Watchlists",
                    "sl_host_country" : 'India',
                    "sl_type" : "Sanctions",
                    "sl_source" : "National Investigation Agency Most Wanted, India",
                    "sl_description" : "The consolidated list of absconders who represent the biggest threats to the safety of Indian community issued by National Investigation Agency."
                }
            }
        )

    global total_profile_available,out_list
    total_profile_available = len(final_list1)
    out_list = deepcopy(final_list1)
    print(f"Total Available Profiles : {total_profile_available}")
 
    print("Saving the json file...")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile,ensure_ascii=False,indent=2)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
                json.dump(out_list, outfile,ensure_ascii=False,indent=2)              


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
    new_list = deepcopy(out_list)

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
                            print(f"Updataion Detected on: {val1['uid']} for: {i}")
                            # print(f"Updataion Detected for: {val1[i]}")
                            updated_profiles.append(val1)
                            break
                    except:
                        print(f"Updataion Detected on: {val1['uid']} for: {i}")
                        # print(f"Updataion Detected for: {val1[i]}")
                        updated_profiles.append(val1)
                        break

        else:
            new_profiles.append(val1)
            print(f"New Profile Detected : {val1['uid']}")
    

    for val2 in old_dict.keys():
        if val2 not in new_uid_list:
            print(f"Removed Profile Detected : {val2}")
            removed_profiles.append(old_dict[val2])

    print("------------------------LOG-DATA---------------------------")
    print(f"total New Profiles Detected:     {len(new_profiles)}")
    print(f"total Updated Profiles Detected: {len(updated_profiles)}")
    print(f"total Removed Profiles Detected: {len(removed_profiles)}")
    print("-----------------------------------------------------------")

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
CompareDocument()
UploadfilestTos3()