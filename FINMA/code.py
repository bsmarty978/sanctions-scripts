import requests
import json
from scrapy.http import HtmlResponse
import os
import time as t
import boto3
import copy
import hashlib
from datetime import datetime,date,timedelta
from os.path import exists
import traceback

class Data():
    try:
        #NOTE: Object for output json file
        out_list = []
        total_profile_available = 0

        #NOTE: Filename according to the date :
        today_date  = date.today()
        yesterday = today_date - timedelta(days = 1)
        last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        dag_name = "finma-ch"

        # input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
        output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
        diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
        removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
        old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
        lp_name = f'{dag_name}-logfile.csv'
        #NOTE: Paths of directories
        root = "/home/ubuntu/sanctions-scripts/FINMA/"
        # root = ""
        # ip_path = f"{root}inputfiles"
        op_path = f"{root}outputfiles"
        dp_path = f"{root}diffrancefiles"
        rm_path = f"{root}removedfiles"
        lp_path = f"{root}{dag_name}-logfile.csv"

    except:
        pass
    
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
        new_list = copy.deepcopy(self.out_list)

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

    def parsing(self):
        try:
            url = "https://www.finma.ch/en/api/search/getresult"

            payload = "ds=%7B77BC25B0-F2B7-4AA2-B0D8-5837913D9B93%7D&Order=1"
            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
                'Cookie': 'AL_SESS-S=Ac2E2Kr8y1hzZ0H4GLj0oA6KsbdKfK!4AVAdF1VM9vJbEZ53Bc2NIjEnCC5CHcXpNC6E; ASP.NET_SessionId=$xc/gXYQGmPMucWpx1e0RFNnMUsUnnEYVlgsVfmxbMofpSOno2ZJ; SC_ANALYTICS_GLOBAL_COOKIE=$xc/CarAyw__kbLXsMi!aDj1UfwHCRubIrV2KGqe9a8A34FybEtakvD9kwpXTEXkBieXlfoUQg==; finma_main#lang=$xc/AIqI1PMXqvqoWRCwUDf!4kPXo7Y=; shell#lang=$xc/AIqI1PMXqvqoWRCwUDf!4kPXo7Y='
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            t.sleep(5)
            body = json.loads(response.text)
            try:
                block = body['Items']
                for i in block:
                    url = "https://www.finma.ch" + i['Link']
                    res = requests.get(url=url,headers=headers,data=payload)
                    response = HtmlResponse(url="example.com",body=res.content)
                    try:
                        name = response.xpath('//*[contains(text(),"Name")]/../td/text()').extract_first(default="").strip()
                    except:
                        name = ""
                    try:
                        address = response.xpath('//*[contains(text(),"Address")]/../td/text()').extract_first(default="").strip()
                    except:
                        address = ""
                    try:
                        website = response.xpath('//*[contains(text(),"Internet")]/../td//text()').extract_first(default="").strip()
                    except:
                        website = ""
                    try:
                        comment = response.xpath('//*[contains(text(),"Commercial register")]/../td//text()').extract_first(default="").strip()
                    except:
                        comment = ""
                    item = {}
                    item = {}
                    item['uid'] = hashlib.sha256(((name + address).lower()).encode()).hexdigest()
                    item['name'] = name
                    item['alias_name'] = []
                    item['country'] = []
                    item['list_type'] = "Entity"
                    item['last_updated'] = self.last_updated_string
                    item['entity_details'] = {}
                    item['entity_details']['website'] = website
                    item['nns_status'] = False
                    item['address'] = []
                    add = {}
                    add['complete_address'] = address
                    add['country'] = ""
                    item['address'].append(add)
                    item['documents'] = {}
                    item['comment'] = comment
                    item['sanction_list'] = {}
                    item['sanction_list']['sl_authority'] = "Swiss Financial Market Supervisory Authority FINMA, Switzerland"
                    item['sanction_list']['sl_url'] = "https://www.dea.gov/fugitives/all"
                    item['sanction_list']['sl_host_country'] = "Switzerland"
                    item['sanction_list']['sl_type'] = "Sanctions"
                    item['sanction_list']['sl_source'] = "Swiss Financial Market Supervisory Authority FINMA Unauthorized Institutions, Switzerland"
                    item['sanction_list']['sl_description'] = "List of warning list of companies who may be carrying out unauthorized services and are not supervised by by The Swiss Financial Market Supervisory Authority FINMA, Switzerland."
                    item['sanction_list']['watch_list'] = "European Watchlists"
                    item['list_id'] = "CHE_E20287"
                    if name!= r"{{=item.CompanyName}}":
                        self.out_list.append(item)
            except Exception as e:
                print(f"Error-1:{traceback.print_exc()}")
                print(e)
            
            self.total_profile_available = len(self.out_list)
            print(f"Total profile available : {self.total_profile_available}")
            try:
                with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                    json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)
            except FileNotFoundError:
                os.mkdir(self.op_path)
                with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                        json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)  

        except Exception as e:
            print(f"Error-2:{traceback.print_exc()}")
            print(e)



temp = Data()
temp.parsing()
temp.CompareDocument()
temp.UploadfilestTos3()
