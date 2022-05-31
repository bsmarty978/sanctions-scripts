import requests
import json
import hashlib
from datetime import datetime,timedelta,date
from os.path import exists
import os
import boto3


p = 1
maxp = 100
failed_page = []
final_failed_p   = []

#NOTE: Filename according to the date :
out_list = []
today_date  = date.today()
yesterday = today_date - timedelta(days = 1)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

dag_name = "cibil"
root = "/home/ubuntu/sanctions-scripts/CIBIL/"

# input_filename  = f'{dag_name}-inout-{today_date}.csv'
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
    return hashlib.sha256(((n+"rbi willfull list IND_E20010").lower()).encode()).hexdigest()


def process_data():
    global p,maxp,out_list,last_updated_string,total_profile_available,failed_page,final_failed_p

    while True:
        print(f"-->{p}")
        pass_dict = {
            "uid": "",
            "name": "",
            "alias_name":[],
            "country": ["India"],
            "list_type": "Entity",
            "last_updated": last_updated_string,
            "entity_details": {},
            "relationship_details" : {
                "associates" : []
            },
            "nns_status": "False",
            "address": [],
            "sanction_details": {},
            "documents": {},
            "comment":"",
            "sanction_list": {
                "sl_authority": "Reserve Bank of India, India",
                "sl_url": "https://suit.cibil.com/loadSuitFiledDataSearchAction",
                "watch_list": "India Watchlists",
                "sl_host_country": "India",
                "sl_type": "Sanctions",
                "list_id": "IND_E20010",
                "sl_source": "Reserve Bank of India Willful Defaulters List, India",
                "sl_description": "List of Will full defaulters issued by Reserve Bank of India and sourced from CIBIL Suit Filed Accounts."
            }
        }
        if p >= maxp:
            break
        all_url = "https://suit.cibil.com/loadSearchResultPage?fileType=2&suitSearchBeanJson={%22borrowerName%22:%22%22,%22costAmount%22:%22%22,%22stateName%22:%22%22,%22directorName%22:%22%22,%22branchBean%22:null,%22dunsNumber%22:%22%22,%22city%22:%22%22,%22bankBean%22:{%22bankId%22:null,%22bankName%22:%22%22,%22categoryBean%22:{%22categoryId%22:0,%22categoryName%22:%22%22,%22categoryAllotedId%22:%22%22,%22active%22:0,%22enable%22:false},%22bankNoRecords%22:0,%22bankTotalAmount%22:%22%22,%22enable%22:false,%22active%22:0},%22quarterBean%22:{%22quarterId%22:null,%22quarterDate%22:null,%22quarterDateStr%22:%22%22,%22quarterName%22:%22%22,%22quarterMonthStr%22:%22%22,%22quarterYearStr%22:%22%22,%22isPush%22:0},%22stateBean%22:{%22stateId%22:null,%22stateName%22:%22%22,%22stateNoRecords%22:0,%22stateTotalAmount%22:%22%22,%22category%22:%22%22,%22enable%22:false,%22isActive%22:0},%22borrowerAddress%22:null,%22borrowerId%22:0,%22sort%22:0,%22totalRecords%22:0,%22totalAmount%22:%22%22,%22quarterCol%22:%22%22,%22categoryBean%22:null,%22noOFCGs1Cr%22:0,%22records1Cr%22:0,%22noOFCGs25Lac%22:0,%22records25Lac%22:0,%22cat%22:%22%22,%22catGroup%22:%22%22,%22fromQuarterId%22:0,%22toQuarterId%22:0,%22partyTypeId%22:0,%22quarterId%22:0,%22srNo%22:%22%22,%22userComments%22:%22%22,%22importDataBean%22:null,%22rejected%22:0,%22user%22:null,%22uploadBatchBean%22:null,%22rejectComment%22:%22%22,%22lastLimit%22:0,%22firstLimit%22:0,%22reject%22:null,%22edit%22:null,%22modify%22:null,%22editedTotalAmount%22:null,%22editedDirectorNames%22:null,%22rejectComments%22:null,%22updateReject%22:%22%22,%22userId%22:0,%22directorBean%22:null,%22isReview%22:%22%22,%22sortBy%22:null,%22sortOrder%22:null,%22summaryState%22:%221%22,%22summaryType%22:%221%22,%22directorId%22:0,%22directorSuffix%22:%22%22,%22dinNumber%22:%22%22,%22editedDirectorDin%22:null,%22dirPan%22:%22%22,%22editedDirectorPan%22:null,%22title%22:%22%22}&_search=false&nd=1653891673837&rows=5000&page="
        l = str(p)
        k = "&sidx=&sord=asc"
        res = requests.get(all_url+l+k)
        try:
            data = json.loads(res.text)
            maxp = int(data["total"])
            for j in data["rows"]:
                # out_list.append(j["borrowerName"])
                name = j["borrowerName"]
                state = j["stateName"]
                direcs = j["directorName"].split(",")

                bank = j["bankBean"]["bankName"]
                total_amount = j["bankBean"]["bankTotalAmount"]

                compadr = j["importDataBean"]["regaddr"] if j["importDataBean"]["regaddr"] else ""
                adrstate = j["importDataBean"]["state"] if j["importDataBean"]["state"] else ""

                adr = {
                    "complete_address" : compadr,
                    "state" : adrstate
                }

                if not name:
                    continue

                pass_dict["uid"] = get_hash(name)
                pass_dict["name"] = name
                pass_dict["address"].append(adr)
                pass_dict["relationship_details"]["associates"] = direcs
                pass_dict["comment"] = f"BANK : {bank} , Total Amount : {total_amount}"

                out_list.append(pass_dict)    

        except:
            try:
                res = requests.get(all_url+l+k)
                data = json.loads(res.text)
                maxp = int(data["total"])
                for j in data["rows"]:
                    # out_list.append(j["borrowerName"])
                    name = j["borrowerName"]
                    state = j["stateName"]
                    direcs = j["directorName"].split(",")

                    bank = j["bankBean"]["bankName"]
                    total_amount = j["bankBean"]["bankTotalAmount"]

                    compadr = j["importDataBean"]["regaddr"] if j["importDataBean"]["regaddr"] else ""
                    adrstate = j["importDataBean"]["state"] if j["importDataBean"]["state"] else ""

                    adr = {
                        "complete_address" : compadr,
                        "state" : adrstate
                    }

                    if not name:
                        continue

                    pass_dict["uid"] = get_hash(name)
                    pass_dict["name"] = name
                    pass_dict["address"].append(adr)
                    pass_dict["relationship_details"]["associates"] = direcs
                    pass_dict["comment"] = f"BANK : {bank} , Total Amount : {total_amount}"

                    out_list.append(pass_dict)    

            except:
                print(f"failed for -{p}")
                failed_page.append(p)
        p+=1
        # print(data["records"])

    for fp in failed_page:
        print(f"-->{fp}")
        all_url = "https://suit.cibil.com/loadSearchResultPage?fileType=2&suitSearchBeanJson={%22borrowerName%22:%22%22,%22costAmount%22:%22%22,%22stateName%22:%22%22,%22directorName%22:%22%22,%22branchBean%22:null,%22dunsNumber%22:%22%22,%22city%22:%22%22,%22bankBean%22:{%22bankId%22:null,%22bankName%22:%22%22,%22categoryBean%22:{%22categoryId%22:0,%22categoryName%22:%22%22,%22categoryAllotedId%22:%22%22,%22active%22:0,%22enable%22:false},%22bankNoRecords%22:0,%22bankTotalAmount%22:%22%22,%22enable%22:false,%22active%22:0},%22quarterBean%22:{%22quarterId%22:null,%22quarterDate%22:null,%22quarterDateStr%22:%22%22,%22quarterName%22:%22%22,%22quarterMonthStr%22:%22%22,%22quarterYearStr%22:%22%22,%22isPush%22:0},%22stateBean%22:{%22stateId%22:null,%22stateName%22:%22%22,%22stateNoRecords%22:0,%22stateTotalAmount%22:%22%22,%22category%22:%22%22,%22enable%22:false,%22isActive%22:0},%22borrowerAddress%22:null,%22borrowerId%22:0,%22sort%22:0,%22totalRecords%22:0,%22totalAmount%22:%22%22,%22quarterCol%22:%22%22,%22categoryBean%22:null,%22noOFCGs1Cr%22:0,%22records1Cr%22:0,%22noOFCGs25Lac%22:0,%22records25Lac%22:0,%22cat%22:%22%22,%22catGroup%22:%22%22,%22fromQuarterId%22:0,%22toQuarterId%22:0,%22partyTypeId%22:0,%22quarterId%22:0,%22srNo%22:%22%22,%22userComments%22:%22%22,%22importDataBean%22:null,%22rejected%22:0,%22user%22:null,%22uploadBatchBean%22:null,%22rejectComment%22:%22%22,%22lastLimit%22:0,%22firstLimit%22:0,%22reject%22:null,%22edit%22:null,%22modify%22:null,%22editedTotalAmount%22:null,%22editedDirectorNames%22:null,%22rejectComments%22:null,%22updateReject%22:%22%22,%22userId%22:0,%22directorBean%22:null,%22isReview%22:%22%22,%22sortBy%22:null,%22sortOrder%22:null,%22summaryState%22:%221%22,%22summaryType%22:%221%22,%22directorId%22:0,%22directorSuffix%22:%22%22,%22dinNumber%22:%22%22,%22editedDirectorDin%22:null,%22dirPan%22:%22%22,%22editedDirectorPan%22:null,%22title%22:%22%22}&_search=false&nd=1653891673837&rows=5000&page="
        l = str(fp)
        k = "&sidx=&sord=asc"
        try:
            res = requests.get(all_url+l+k)
            data = json.loads(res.text)
            maxp = int(data["total"])
            for j in data["rows"]:
                # out_list.append(j["borrowerName"])
                name = j["borrowerName"]
                state = j["stateName"]
                direcs = j["directorName"].split(",")

                bank = j["bankBean"]["bankName"]
                total_amount = j["bankBean"]["bankTotalAmount"]

                compadr = j["importDataBean"]["regaddr"] if j["importDataBean"]["regaddr"] else ""
                adrstate = j["importDataBean"]["state"] if j["importDataBean"]["state"] else ""

                adr = {
                    "complete_address" : compadr,
                    "state" : adrstate
                }

                if not name:
                    continue

                pass_dict["uid"] = get_hash(name)
                pass_dict["name"] = name
                pass_dict["address"].append(adr)
                pass_dict["relationship_details"]["associates"] = direcs
                pass_dict["comment"] = f"BANK : {bank} , Total Amount : {total_amount}"

                out_list.append(pass_dict)    

        except:
            print(f"failed for -{fp}")
            final_failed_p.append(fp)

    total_profile_available = len(out_list)
    print(f"Total profile available: {total_profile_available}")
    try:
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)
    except FileNotFoundError:
        os.mkdir(op_path)
        with open(f'{op_path}/{output_filename}', "w",encoding='utf-8') as outfile:
            json.dump(out_list, outfile, ensure_ascii=False, indent=4)

    with open(f"{root}cibil-failed-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
        json.dump(final_failed_p, outfile, ensure_ascii=False, indent=4)

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
CompareDocument()
UploadfilestTos3() 

        