import requests
from scrapy.http import HtmlResponse
import camelot
from datetime import datetime,date,timedelta
import json
import hashlib
import os
from os.path import exists
from copy import deepcopy
import boto3

class mcagov():
    out_list = []
    fin_list = []
    raw_all_dict = {}
    # input_list = []
    total_profile_available = 0

    #NOTE: Filename according to the date :
    today_date  = date.today()
    yesterday = today_date - timedelta(days = 1)
    last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    dag_name = "mca-s"
    root = "/home/ubuntu/sanctions-scripts/MCA-S/"

    # input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
    output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
    diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
    removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
    old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
    lp_name = f'{dag_name}-logfile.csv'
    #NOTE: Paths of directories
    # root = ""
    # ip_path = f"{root}inputfiles"
    op_path = f"{root}outputfiles"
    dp_path = f"{root}diffrancefiles"
    rm_path = f"{root}removedfiles"
    lp_path = f"{root}{dag_name}-logfile.csv"

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
        new_list = deepcopy(self.out_list)

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

    def alias_name(self,nm):
        a = nm.strip().split(" ")
        if len(a)>1:
            try:
                ret_name =  f"{a[2]} {a[0]} {a[1]}"
            except:
                ret_name =  f"{a[1]} {a[0]}"
            
        else:
            ret_name = ""
        
        return ret_name  
        

    def parsing(self):
        counter = 0
        url = "https://www.mca.gov.in/MinistryV2/defaultersecretarieslist.html"

        payload = {}
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
        }
        res = requests.get(url, headers=headers, data=payload)
        response = HtmlResponse(url="example.com",body=res.content)
        body = response.xpath('//*[@class="rightInnerContent floatRight"]//a/@href').extract()
        # body = response.xpath('//*[@class="resposive_tablet3"]//a/@href').extract()
        for i in body:
            counter = counter + 1
            link = "https://www.mca.gov.in" + i
            res = requests.get(link, headers=headers, data=payload)
            f = open(f"mca.gov.in_697_{counter}.pdf", "wb")
            f.write(res.content)
            f.close()

            tb = camelot.read_pdf(f"mca.gov.in_697_{counter}.pdf", pages="all", flavor="stream")
            print(f"Tables :{len(tb)}")
            for i in tb:
                df = i.df
                for i,row in df.iterrows():
                    try:
                        signatory_id = row[0]
                    except:
                        signatory_id = ""
                    try:
                        name = row[1]
                    except:
                        name = ""
                    try:
                        cin = row[2]
                    except:
                        cin = ""
                    try:
                        company_name = row[3]
                    except:
                        company_name = ""
                    try:
                        default_year = row[4]
                    except:
                        default_year = ""

                    item = {}
                    if name != "Name":
                        if signatory_id == "":
                            self.fin_list[-1]["name"] = self.fin_list[-1]["name"]+ " " + name
                            self.fin_list[-1]["comment"] =  self.fin_list[-1]["comment"] + " " +company_name 
                            continue
                        else:
                            item['uid'] = hashlib.sha256(((signatory_id+name+"MCA-Secretaries").lower()).encode()).hexdigest()
                            item['name'] = name
                            item['alias_name'] = [self.alias_name(name)]
                            item['country'] = ["India"]
                            item['list_type'] = "Individual"
                            item['last_updated'] = self.last_updated_string
                            item['individual_details'] = {}
                            item['sanction_details'] = {}
                            item['nns_status'] = False
                            item['address'] = []
                            item['documents'] = {}
                            item["documents"]["CIN"] = [cin]
                            item['comment'] = company_name
                            item['sanction_list'] = {}
                            item['sanction_list']['sl_authority'] = "Ministry of Corporate Affairs, India"
                            item['sanction_list']['sl_url'] = "https://www.mca.gov.in/MinistryV2/defaultersecretarieslist.html"
                            item['sanction_list']['sl_host_country'] = "India"
                            item['sanction_list']['sl_type'] = "Sanctions"
                            item['sanction_list']['watch_list'] = "India Watchlists"
                            item['sanction_list']['sl_source'] = "Ministry of Corporate Affairs Defaulter Secretaries List, India"
                            item['sanction_list']['sl_description'] = "List of defaulter Directors by Ministry of Corporate Affairs, India."
                            item['sanction_list']['list_id'] = "IND_E20296"
                            self.fin_list.append(item)
            #     break
            # break
        for k in self.fin_list:
            if k["uid"] in list(self.raw_all_dict.keys()):
                self.raw_all_dict[k["uid"]]["documents"]["CIN"] .append(k["documents"]["CIN"][0])
                c_name = self.raw_all_dict[k["uid"]]['comment']
                self.raw_all_dict[k["uid"]]['comment'] = c_name + ", " + k['comment']
            else: 
                self.raw_all_dict[k["uid"]] = k 

        # ab = json.dumps(self.out_list)
        # ab = json.dumps(list(self.raw_all_dict.values()),indent=2,ensure_ascii=False)
        # f = open(f"{self.outputpath}/{self.date}_output_www.mca.gov.in_696.json", "w")
        # f.write(ab)
        # f.close()

        self.out_list = deepcopy(list(self.raw_all_dict.values()))
        self.total_profile_available = len(self.out_list)
        print(f"Total profile available : {self.total_profile_available}")
        try:
            with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)
        except FileNotFoundError:
            os.mkdir(self.op_path)
            with open(f'{self.op_path}/{self.output_filename}', "w",encoding='utf-8') as outfile:
                    json.dump(self.out_list, outfile,ensure_ascii=False,indent=2)  


temp = mcagov()
temp.parsing()
temp.CompareDocument()
temp.UploadfilestTos3()
