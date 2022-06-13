import requests
import json
from datetime import datetime,date,timedelta
from scrapy.http import HtmlResponse
import os
from os.path import exists
import pandas as pd
import time as t
import boto3
import hashlib


#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "mcx-sm"
root = "/home/ubuntu/sanctions-scripts/MCX-SM/"

input_filename  = f'{dag_name}-inout-{today_date}.xlsx'
output_filename = f'{dag_name}-output-{today_date}.json'
diffrance_filename = f'{dag_name}-diffrance-{today_date}.json'
removed_filename = f'{dag_name}-removed-{today_date}.json'
old_output_filename = f'{dag_name}-output-{yesterday}.json'
lp_name = f'{dag_name}-logfile.csv'
#NOTE: Paths of directories
# root = ""
ip_path = f"{root}inputfiles"
op_path = f"{root}outputfiles"
dp_path = f"{root}diffrancefiles"
rm_path = f"{root}removedfiles"
lp_path = f"{root}{dag_name}-logfile.csv"

def get_hash(n):
    return hashlib.sha256(((n+"MCX clients of the Defaulter Members, India IND_E20316").lower()).encode()).hexdigest()

def sourcedownloader():
    r = requests.get("https://www.mcxindia.com/membership/notice-board/list-of-surrender-members")
    resp = HtmlResponse("example.com",body=r.text,encoding='utf-8')
    url = "https://www.mcxindia.com"+ resp.xpath("//a[@title='List of Surrender/Ceased Members']/@href").get()
    
    headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }
    res = requests.get(url,headers=headers)

    try:
        with open(f'{ip_path}/{input_filename}', "wb") as infile:
            infile.write(res.content)
    except FileNotFoundError:
        os.mkdir(ip_path)
        with open(f'{ip_path}/{input_filename}', "wb") as infile:
            infile.write(res.content)


def alias_name(name):
    alias_list=[]
    rearrangedNamelist=name.split(' ')
    lastname= rearrangedNamelist.pop(-1)
    rearrangedNamelist=[lastname] + rearrangedNamelist
    alias_list.append(' '.join(rearrangedNamelist))
    return alias_list 

def process_data():
    global out_list,last_updated_string,total_profile_available
    df = pd.read_excel(f'{ip_path}/{input_filename}',sheet_name='Surrender of Membership')
    df.fillna("",inplace=True)
    # data = df.values.tolist()
    # print(df.head())
    # exit()
    for i in range(len(df)):
        item = {
            "uid": "",
            "name": "",
            "alias_name": [],
            "country": ["India"],
            "list_type": "",
            "last_updated": last_updated_string,
            "nns_status": "False",
            "address": [
                {
                "country": "",
                "complete_address": ""
                }
            ],
            "sanction_details":{},
            "documents": {},
            "comment": "",
            "sanction_list": {
                "sl_authority": "MCX Surrendered/Ceased Members List, India",
                "sl_url": "https://www.mcxindia.com/membership/notice-board/list-of-surrender-members",
                "watch_list": "India Watchlists",
                "sl_host_country": "India",
                "sl_type": "Sanctions",
                "sl_source": "MCX Surrendered/Ceased Members List, India",
                "list_id" : "IND_E20318",
                "sl_description": "list of Surrendered/Ceased Members by Multi Commodity Exchange of India Ltd, India",}
        }
                
        try:
            name = df.iloc[i, 1].strip()
        except:
            name = ""
            continue
        try:
            member_id = str(df.iloc[i, 2]).replace("-","").strip()
        except:
            member_id = ''
        try:
            sebi_no = str(df.iloc[i, 3]).replace("-","").strip()
        except:
            sebi_no = ''
        try:
            date = df.iloc[i, 4]
        except:
            date = ''
        
        try:
            check = False
            aa = ['Creations','Chains','Centre','Investors','Comtech','Hub','Agency','Derivatives','Gems','Broker','Chemicals','Market','VSM','India','Com','Vifco','Plastics','ALCOM','INDIA','Metals','BROKERS','PRIVATE','Ventures','LTD.','Limiteed','Sons','Commedties','Financial','Comtrade','LLP','Polymers','Products','Gold','Inc.','Bullion','Links','Link','Equicom','Management','Impex','Technologies','Holdings','Mills','Impex','Commodites','Ltd.','International','Limited','Traders','Brokers','Enterprises','Commodities','Corporation','Industries','&','Ltd','Brocom','Care','Investment','Securities','Company','Trade','Polychem','Pvt ltd','Commodity','Limited','Private limited','Co.','Trading','Max Wealth','Jewellery','Finance','Agencies','Equity','Broking','Brothers','Associates','Hedging','Overseas','Tradex','Intermediaries','Services','Fincorp','Exports','SERVICE','Merchant','Futures','Enterprise','Consultancy','Capital','Jewellers','Corp.','Corp','Agromet']
            for a in aa:
                if a in name:
                    check = True
                    list_type = 'Entity'
                    item['entity_details'] = {}
            if not check:
                list_type = 'Individual'
                item['individual_details'] = {}
        except Exception as e:
            print(e)

        item['uid'] = hashlib.sha256(((name+ "MCX Surrendered/Ceased Members List, India IND_E20318").lower()).encode()).hexdigest()
        item['name'] = name
        item['list_type'] = list_type
        item['nns_status'] = False
        item['sanction_details']['issue_date'] = date
        item['documents'] = {}
        item['documents']['other_id'] = []
        if member_id:
            item['documents']['other_id'].append(str(member_id))
        if sebi_no:
            item['documents']['other_id'].append(str(sebi_no))
        out_list.append(item)

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
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(passing)
    else:
        with open(f'{lp_path}',"a") as outfile:
            pass_first = "date,inputfile,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
            passing = f"{last_updated_string},{input_filename},{output_filename},{total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{diffrance_filename},{removed_filename}\n"
            outfile.write(pass_first)
            outfile.write(passing)

def UploadfilestTos3():
    try:
        print("uploading files to s3")
        s3 = boto3.client('s3')
        s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
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

sourcedownloader()
process_data()
CompareDocument()
UploadfilestTos3() 

        

