import requests
import json
from scrapy.http import HtmlResponse
from datetime import datetime,date,timedelta
import os
from os.path import exists
import time as t
import boto3
import copy
import hashlib


class Data():
    out_list = []
    total_profile_available = 0
    today_date  = date.today()
    yesterday = today_date - timedelta(days = 1)
    last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    dag_name = "ofac-civil"
    root = "/home/ubuntu/sanctions-scripts/OFAC-CIVIL/"

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

    def parsing(self):
        urls = ["https://home.treasury.gov/policy-issues/financial-sanctions/civil-penalties-and-enforcement-information",
                "https://home.treasury.gov/policy-issues/financial-sanctions/civil-penalties-and-enforcement-information/2021-enforcement-information",
                "https://home.treasury.gov/policy-issues/financial-sanctions/civil-penalties-and-enforcement-information/2020-enforcement-information",
                "https://home.treasury.gov/policy-issues/financial-sanctions/civil-penalties-and-enforcement-information/2019-enforcement-information"]
        for url in urls:

            # payload = {}
            # headers = {
            #     'Cookie': 'PHPSESSID=af56ab0690cba426b0cc6c15027b89ab; TS01b6fa6b=01885b66e8083558d8eaf949ade0cfcf75a46c3a69088d1f4d4449f9cc99a84c036c294521c0b623e42f2fa621ca98fce3e457dadce5be3b1125df83ae024650a0806c8139'
            # }

            res = requests.get(url)
            response = HtmlResponse(url="example.com", body=res.content)
            # print(response)

            divs = response.xpath('//tbody//a/../..')
            for div in divs:
                try:
                    name = div.xpath('./td[1]/text()').extract_first(default="").strip()
                except:
                    name = ""
                # print(name)
                try:
                    penalties = div.xpath('./td[3]/text()').get()
                except Exception as e:
                    print(e)
                    penalties = ""
                item = {}
                item['uid'] = hashlib.sha256(((name + "Civil Penalties and Enforcement Information, USA USA_T30037").lower()).encode()).hexdigest()
                item['name'] = name
                item['alias_name'] = []
                item['country'] = []
                item['list_type'] = "Entity"
                item['last_updated'] = self.last_updated_string
                item['entity_details'] = {}
                # item['entity_details']['website'] = []
                item['nns_status'] = False
                item['address'] = []
                add = {}
                add['complete_address'] = ""
                add['country'] = ""
                item['address'].append(add)
                item['relationship_details'] = {}
                item['sanction_details'] = {}
                item['sanction_details']['penalties_amount'] = penalties
                item['documents'] = {}
                item['comment'] = []
                item['sanction_list'] = {}
                item['sanction_list']['sl_authority'] = "U.S. DEPARTMENT OF THE TREASURY"
                item['sanction_list']['sl_url'] = "https://home.treasury.gov/"
                item['sanction_list']['sl_host_country'] = "USA"
                item['sanction_list']['sl_type'] = "Sanctions"
                item['sanction_list'][
                    'sl_source'] = "Civil Penalties and Enforcement Information, USA"
                item['sanction_list'][
                    'sl_description'] = "Civil Penalties and Enforcement Information, USA"
                item['sanction_list']['watch_list'] = "North America Watchlists"
                item['sanction_list']['list_id'] = "USA_T30037"
                self.out_list.append(item)

        self.total_profile_available = len(self.out_list)
        print(f"Total profile available: {self.total_profile_available}")
        try:
            with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                json.dump(self.out_list, outfile, ensure_ascii=False, indent=4)
        except FileNotFoundError:
            os.mkdir(self.op_path)
            with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                json.dump(self.out_list, outfile, ensure_ascii=False, indent=4)        


    def CompareDocument(self):
        try:
            with open(f'{self.op_path}/{self.old_output_filename}', 'rb') as f:
                data = f.read()
        except:
            print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            print("----------------------------------------------------")
            data = "No DATA"
        old_list = []
        if data != "No DATA":   
            old_list = json.loads(data)

        with open(f'{self.op_path}/{self.output_filename}', 'rb') as f:
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
            with open(f'{self.dp_path}/{self.diffrance_filename}', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.dp_path)
            with open(f'{self.dp_path}/{self.diffrance_filename}', "w",encoding='utf-8') as outfile:
                json.dump(new_profiles+updated_profiles, outfile,ensure_ascii=False)

        try:
            with open(f'{self.rm_path}/{self.removed_filename}', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)
        except FileNotFoundError:
            os.mkdir(self.rm_path)
            with open(f'{self.rm_path}/{self.removed_filename}', "w",encoding='utf-8') as outfile:
                json.dump(removed_profiles, outfile,ensure_ascii=False)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.output_filename},{self.total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.diffrance_filename},{self.removed_filename}\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,outputfile,total_profile_availabe,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.output_filename},{self.total_profile_available},{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.diffrance_filename},{self.removed_filename}\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def UploadfilestTos3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            # s3.upload_file(f'{ip_path}/{input_filename}',"sams-scrapping-data",f"{dag_name}/original/{input_filename}")
            s3.upload_file(f'{self.op_path}/{self.output_filename}',"sams-scrapping-data",f"{self.dag_name}/parced/{self.output_filename}")
            s3.upload_file(f'{self.dp_path}/{self.diffrance_filename}',"sams-scrapping-data",f"{self.dag_name}/diffrance/{self.diffrance_filename}")
            s3.upload_file(f'{self.rm_path}/{self.removed_filename}',"sams-scrapping-data",f"{self.dag_name}/removed/{self.removed_filename}")
            s3.upload_file(f'{self.lp_path}',"sams-scrapping-data",f"{self.dag_name}/{self.lp_name}")
            print("uploaded files to s3")      
        except Exception as e:
            print("------------------🔴ALERT🔴------------------------")
            print("Can not upload files to s3")
            print("Exception : " , e)
            print("----------------------------------------------------")


temp = Data()
temp.parsing()
temp.CompareDocument()
# temp.uploadToS3()
