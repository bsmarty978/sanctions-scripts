import requests
import json
import os
from scrapy.http import HtmlResponse
import hashlib
import boto3
import copy
from datetime import datetime,date,timedelta
from os.path import exists





class Data():
    try:
        #NOTE: Object for output json file
        out_list = []
        total_profile_available = 0

        #NOTE: Filename according to the date :
        today_date  = date.today()
        yesterday = today_date - timedelta(days = 1)
        last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        dag_name = "usa-dea"

        # input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
        output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
        diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
        removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
        old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
        lp_name = f'{dag_name}-logfile.csv'
        #NOTE: Paths of directories
        root = "/home/ubuntu/sanctions-scripts/USA-DEA/"
        # root = ""
        # ip_path = f"{root}inputfiles"
        op_path = f"{root}outputfiles"
        dp_path = f"{root}diffrancefiles"
        rm_path = f"{root}removedfiles"
        lp_path = f"{root}{dag_name}-logfile.csv"
        
    except:
        pass
    def uploadToS3(self):
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
        for i in range(0,70):
            try:
                url = f"https://www.dea.gov/fugitives/all?keywords=&page={i}"

                payload = {}
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
                }

                res = requests.get(url, headers=headers, data=payload)
                responsee = HtmlResponse(url="example.com",body=res.content)
                try:
                    links = responsee.xpath('//*[@class="teaser__heading"]//@href').extract()
                    for j in links:
                        link = "https://www.dea.gov" + j
                        ress = requests.get(link, headers=headers, data=payload)
                        response = HtmlResponse(url="example.com", body=ress.content)
                        try:
                            name = response.xpath('//*[@class="fugitive__title"]//text()').extract_first(default="").strip()
                        except:
                            name = ""
                        try:
                            alias_name = response.xpath('//*[@class="meta"][1]/div//text()').extract_first(default="").strip()
                        except:
                            alias_name = ""
                        try:
                            commet = ''.join(response.xpath('//*[@class="meta"][2]//text()').extract()).strip().replace("\n","")
                        except:
                            commet = ""
                        try:
                            dob = response.xpath('//*[contains(text(),"Year of Birth")]/..//td[2]//text()').extract_first(default="").strip()
                        except:
                            dob = ""
                        try:
                            address = response.xpath('//*[contains(text(),"Last Known Address")]/..//td[2]//text()').extract_first(default="").strip()
                        except:
                            address = ""
                        item = {}
                        item['uid'] = hashlib.sha256(((name).lower()).encode()).hexdigest()
                        item['name'] = name
                        item['alias_name'] = []
                        item['alias_name'].append(name.split(" ")[-1])
                        item['country'] = []
                        item['list_type'] = "Individual"
                        item['last_updated'] = self.last_updated_string
                        item['individual_details'] = {}
                        item['individual_details']['date_of_birth'] = []
                        item['individual_details']['date_of_birth'].append(dob)
                        item['nns_status'] = False
                        item['address'] = []
                        add = {}
                        add['complete_address'] = address
                        add['country'] = ""
                        item['address'].append(add)
                        item['documents'] = {}
                        item['comment'] = commet
                        item['sanction_list'] = {}
                        item['sanction_list']['sl_authority'] = "United States Drug Enforcement Administration, U.S. Department of Justice, USA"
                        item['sanction_list']['sl_url'] = "https://www.dea.gov/fugitives/all"
                        item['sanction_list']['sl_host_country'] = "USA"
                        item['sanction_list']['sl_type'] = "North America Watchlists"
                        item['sanction_list']['sl_source'] = "United States Drug Enforcement Administration, Wanted List, USA"
                        item['sanction_list']['sl_description'] = "List of Wanted Criminals by The United States Drug Enforcement Administration, U.S. Department of Justice, USA."
                        item['sanction_list']['watch_list'] = "North America Watchlists"
                        item['list_id'] = "USA_T30035"
                        self.out_list.append(item)
                except Exception as e:
                    print(e)
            except Exception as e:
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

temp = Data()
temp.parsing()
temp.CompareDocument()
temp.uploadToS3()