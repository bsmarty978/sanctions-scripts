#-*-*-*-*- Rewards Of Justice FOR ENTITY -*-*-*-*-*-*-*

import requests
import json
from scrapy.http import HtmlResponse
import boto3
import hashlib
from datetime import datetime,date,timedelta
import os
from os.path import exists
from copy import deepcopy

class Data():
    try:
        out_list = []
        total_profile_available = 0

        #NOTE: Filename according to the date :
        today_date  = date.today()
        yesterday = today_date - timedelta(days = 1)
        last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        dag_name = "rfj-terrorist"

        # input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'
        output_filename = f'{dag_name}-output-{today_date.day}-{today_date.month}-{today_date.year}.json'
        diffrance_filename = f'{dag_name}-diffrance-{today_date.day}-{today_date.month}-{today_date.year}.json'
        removed_filename = f'{dag_name}-removed-{today_date.day}-{today_date.month}-{today_date.year}.json'
        old_output_filename = f'{dag_name}-output-{yesterday.day}-{yesterday.month}-{yesterday.year}.json'
        lp_name = f'{dag_name}-logfile.csv'
        #NOTE: Paths of directories
        root = "/home/ubuntu/sanctions-scripts/RFJ-1/"
        # root = ""
        # ip_path = f"{root}inputfiles"
        op_path = f"{root}outputfiles"
        dp_path = f"{root}diffrancefiles"
        rm_path = f"{root}removedfiles"
        lp_path = f"{root}{dag_name}-logfile.csv"

        counter = 0
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

    def get_hash(self,n):
        return hashlib.sha256(((n+"Rewards for Justice-Wanted for Terrorism List, Russia").lower()).encode()).hexdigest()  

    def parsing(self):
        p=1
        while True:
            print(p)
            new_url = f"https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=crime-category:1073%2C1072%2C1074&pagenum={p}"
            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
            }
            res = requests.get(new_url, headers=headers, data=payload)
            ress = HtmlResponse(url="example.com",body=res.content)
            d = ress.xpath('//*[@class="jet-engine-listing-overlay-wrap"]')
            if d==[]:
                break
            else:
                for r in d:
                    rlink = r.xpath('.//@data-url').get()
                    rcat  = r.xpath('.//section[1]//h2/text()').get()
                    if rcat in ['Terrorism Financing','Organizations','Terrorism - Individuals']:
                        data = requests.get(rlink,headers=headers, data=payload)
                        resp = HtmlResponse(url="example.com",body=data.content)
                        name = resp.xpath('//*[@class="elementor-column elementor-col-33 elementor-top-column elementor-element elementor-element-7a45ba0"]//h2[@class="elementor-heading-title elementor-size-default"]//text()').extract_first(default="").strip()

                        dob = ""
                        placeofbirth = ""
                        country = ""
                        alias_name = ""
                        Identifications = ""
                        Associated = ""
                        gender = ""
                        if rcat in ["Terrorism - Individuals","Terrorism Financing"]:
                            try:
                                dob = ''.join(resp.xpath('//*[contains(text(),"Date of Birth:")]/../../..//*[@class="elementor-element elementor-element-9a896ea dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                            except:
                                dob = ""
                            try:
                                placeofbirth = ''.join(resp.xpath('//*[contains(text(),"Place of Birth:")]/../../..//*[@class="elementor-element elementor-element-89fe776 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                            except:
                                placeofbirth = ""
                            try:
                                country = ''.join(resp.xpath('//*[contains(text(),"Citizenship:")]/../../..//*[@class="elementor-element elementor-element-fcb86f5 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"Nationality:")]/../../..//*[@class="elementor-element elementor-element-d94db6a dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                            except:
                                country = ""
                            try:
                                alias_name = ''.join(resp.xpath('//*[contains(text(),"Aliases/Alternative Name Spellings:")]/../../..//*[@class="elementor-element elementor-element-50c20df dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                            except:
                                alias_name = ""
                            try:
                                Identifications = resp.xpath('//*[contains(text(),"Miscellaneous Identifications:")]/../../..//*[@class="elementor-element elementor-element-c8cff6e dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"National Identification Number(s) and Country:")]/../../..//*[@class="elementor-element elementor-element-4f362cf dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()
                            except:
                                Identifications = ""
                            try:
                                Associated = resp.xpath('//*[contains(text(),"Associated Organization(s):")]/../../..//a/text()').extract_first(default="").strip()
                            except:
                                Associated = ""
                            try:
                                gender = ''.join(resp.xpath('//*[contains(text(),"Sex:")]/../../..//*[@class="elementor-element elementor-element-11fd5fa elementor-widget elementor-widget-jet-listing-dynamic-terms"]//text()').extract()).strip()
                            except:
                                gender = ""
                    
                        try:
                            comment = ''.join(resp.xpath('//*[@class="elementor-element elementor-element-52b1d20 elementor-widget elementor-widget-theme-post-content"]//p//text()').extract()).strip()
                        except:
                            comment = ""

                        item = {}
                        if dob or placeofbirth or Identifications:
                            item['uid'] = self.get_hash(name)
                            item['name'] = name
                            item['alias_name'] = []
                            if len(alias_name.split(";"))>1:
                                item['alias_name'] = alias_name.split(";")
                            else:
                                item['alias_name'] = alias_name.split(",")
                            item['country'] = []
                            item['list_type'] = "Individual"
                            item['last_updated'] = self.last_updated_string
                            # item['Family_Tree'] = {}
                            # item['Family_Tree']['Associated'] = Associated
                            item['sanction_Details'] = {}
                            # item['sanction_Details']['Associated'] = Associated
                            item['individual_details'] = {}
                            item['individual_details']['date_of_birth'] = dob.split(";")
                            item['individual_details']['Gender'] = gender
                            item['individual_details']['Place_of_Birth'] = placeofbirth
                            item['nns_status'] = False
                            item['address'] = []
                            add = {}
                            # add['country'] = country.replace("\t","").replace("\n","").strip()
                            add['country'] = country.split("\t")[0]
                            add['complete_address'] = ""
                            item['address'].append(add)
                            item['documents'] = {}
                            # try:
                            #     body = [I.strip() for I in Identifications]
                            #     for H in body:
                            #         if H != "":
                            #             data = H.strip().replace("\n","").split(":")
                            #             item['documents'][data[0]] = data[1]
                            #         else:
                            #             continue
                            # except:
                            #     pass
                            item['comment'] = comment
                            item['sanction_list'] = {}
                            item['sanction_list']['sl_authority'] = "Rewards for Justice, USA"
                            item['sanction_list']['sl_url'] = "https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=crime-category:1072"
                            item['sanction_list']['sl_host_country'] = "USA"
                            item['sanction_list']['sl_type'] = "Sanctions"
                            item['sanction_list']['watch_list'] = "North America Watchlists"
                            item['sanction_list']['sl_source'] = "Rewards for Justice-Wanted for Terrorism List, Russia"
                            item['sanction_list']['sl_description'] = "List of most wanted individuals and entities by Rewards for Justice, USA."
                            item['list_id'] = "USA_T30098"
                            self.out_list.append(item)
                        else:
                            item['uid'] = self.get_hash(name)
                            item['name'] = name
                            item['alias_name'] = []
                            item['country'] = []
                            item['list_type'] = "Entity"
                            item['last_updated'] = self.last_updated_string
                            item['entity_details'] = {}
                            item['nns_status'] = False
                            item['address'] = []
                            add = {}
                            add['country'] = ""
                            add['complete_address'] = ""
                            item['address'].append(add)
                            item['documents'] = {}
                            item['comment'] = comment
                            item['sanction_list'] = {}
                            item['sanction_list']['sl_authority'] = "Rewards for Justice, USA"
                            item['sanction_list']['sl_url'] = "https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=crime-category:1073"
                            item['sanction_list']['sl_host_country'] = "USA"
                            item['sanction_list']['sl_type'] = "Sanctions"
                            item['sanction_list']['watch_list'] = "North America Watchlists"
                            item['sanction_list']['sl_source'] = "Rewards for Justice-Wanted for Terrorism List, Russia"
                            item['sanction_list']['sl_description'] = "List of most wanted individuals and entities by Rewards for Justice, USA."
                            item['list_id'] = "USA_T30098"
                            self.out_list.append(item)                    
            p+=1

        print(f'Terrorims : {len(self.out_list)}')
        #NOTE: ELECTRONIC INTERFEARANCE : 
        ep=1
        while True:
            print(ep)
            new_url = f"https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=election-interference:2030&pagenum={ep}"
            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
            }
            res = requests.get(new_url, headers=headers, data=payload)
            ress = HtmlResponse(url="example.com",body=res.content)
            d = ress.xpath('//*[@class="jet-engine-listing-overlay-wrap"]')
            if d==[]:
                break
            else:
                for r in d:
                    rlink = r.xpath('.//@data-url').get()
                    rcat  = r.xpath('.//section[1]//h2/text()').get()
                    # if rcat in ['Terrorism Financing','Organizations','Terrorism - Individuals']:
                    data = requests.get(rlink,headers=headers, data=payload)
                    resp = HtmlResponse(url="example.com",body=data.content)
                    name = resp.xpath('//*[@class="elementor-column elementor-col-33 elementor-top-column elementor-element elementor-element-7a45ba0"]//h2[@class="elementor-heading-title elementor-size-default"]//text()').extract_first(default="").strip()

                    dob = ""
                    placeofbirth = ""
                    country = ""
                    alias_name = ""
                    Identifications = ""
                    Associated = ""
                    gender = ""
                    try:
                        dob = ''.join(resp.xpath('//*[contains(text(),"Date of Birth:")]/../../..//*[@class="elementor-element elementor-element-9a896ea dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        dob = ""
                    try:
                        placeofbirth = ''.join(resp.xpath('//*[contains(text(),"Place of Birth:")]/../../..//*[@class="elementor-element elementor-element-89fe776 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        placeofbirth = ""
                    try:
                        country = ''.join(resp.xpath('//*[contains(text(),"Citizenship:")]/../../..//*[@class="elementor-element elementor-element-fcb86f5 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"Nationality:")]/../../..//*[@class="elementor-element elementor-element-d94db6a dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        country = ""
                    try:
                        alias_name = ''.join(resp.xpath('//*[contains(text(),"Aliases/Alternative Name Spellings:")]/../../..//*[@class="elementor-element elementor-element-50c20df dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        alias_name = ""
                    try:
                        Identifications = resp.xpath('//*[contains(text(),"Miscellaneous Identifications:")]/../../..//*[@class="elementor-element elementor-element-c8cff6e dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"National Identification Number(s) and Country:")]/../../..//*[@class="elementor-element elementor-element-4f362cf dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()
                    except:
                        Identifications = ""
                    try:
                        Associated = resp.xpath('//*[contains(text(),"Associated Organization(s):")]/../../..//a/text()').extract_first(default="").strip()
                    except:
                        Associated = ""
                    try:
                        gender = ''.join(resp.xpath('//*[contains(text(),"Sex:")]/../../..//*[@class="elementor-element elementor-element-11fd5fa elementor-widget elementor-widget-jet-listing-dynamic-terms"]//text()').extract()).strip()
                    except:
                        gender = ""
            
                    try:
                        comment = ''.join(resp.xpath('//*[@class="elementor-element elementor-element-52b1d20 elementor-widget elementor-widget-theme-post-content"]//p//text()').extract()).strip()
                    except:
                        comment = ""

                    item = {}
                    item['uid'] = self.get_hash(name)
                    item['name'] = name
                    item['alias_name'] = []
                    if len(alias_name.split(";"))>1:
                        item['alias_name'] = alias_name.split(";")
                    else:
                        item['alias_name'] = alias_name.split(",")
                    item['country'] = []
                    item['list_type'] = "Individual"
                    item['last_updated'] = self.last_updated_string
                    # item['Family_Tree'] = {}
                    # item['Family_Tree']['Associated'] = Associated
                    item['sanction_Details'] = {}
                    # item['sanction_Details']['Associated'] = Associated
                    item['individual_details'] = {}
                    item['individual_details']['date_of_birth'] = dob.split(";")
                    item['individual_details']['Gender'] = gender
                    item['individual_details']['Place_of_Birth'] = placeofbirth
                    item['nns_status'] = False
                    item['address'] = []
                    add = {}
                    # add['country'] = country.replace("\t","").replace("\n","").strip()
                    add['country'] = country.split("\t")[0]
                    add['complete_address'] = ""
                    item['address'].append(add)
                    item['documents'] = {}
                    # try:
                    #     body = [I.strip() for I in Identifications]
                    #     for H in body:
                    #         if H != "":
                    #             data = H.strip().replace("\n","").split(":")
                    #             item['documents'][data[0]] = data[1]
                    #         else:
                    #             continue
                    # except:
                    #     pass
                    item['comment'] = comment
                    item['sanction_list'] = {}
                    item['sanction_list']['sl_authority'] = "Rewards for Justice, USA"
                    item['sanction_list']['sl_url'] = "https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=crime-category:1072"
                    item['sanction_list']['sl_host_country'] = "USA"
                    item['sanction_list']['sl_type'] = "Sanctions"
                    item['sanction_list']['watch_list'] = "North America Watchlists"
                    item['sanction_list']['sl_source'] = "Rewards for Justice-Wanted for Terrorism List, Russia"
                    item['sanction_list']['sl_description'] = "List of most wanted individuals and entities by Rewards for Justice, USA."
                    item['list_id'] = "USA_T30098"
                    self.out_list.append(item)                
            ep+=1
        print(f'Electronic Interfearance : {len(self.out_list)}')

        #NOTE: CYBER : 
        cp=1
        while True:
            print(cp)
            new_url = f"https://rewardsforjustice.net/index/?jsf=jet-engine:rewards-grid&tax=cyber:857&pagenum={cp}"
            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
            }
            res = requests.get(new_url, headers=headers, data=payload)
            ress = HtmlResponse(url="example.com",body=res.content)
            d = ress.xpath('//*[@class="jet-engine-listing-overlay-wrap"]')
            if d==[]:
                break
            else:
                for r in d:
                    rlink = r.xpath('.//@data-url').get()
                    rcat  = r.xpath('.//section[1]//h2/text()').get()
                    # if rcat in ['Terrorism Financing','Organizations','Terrorism - Individuals']:
                    data = requests.get(rlink,headers=headers, data=payload)
                    resp = HtmlResponse(url="example.com",body=data.content)
                    name = resp.xpath('//*[@class="elementor-column elementor-col-33 elementor-top-column elementor-element elementor-element-7a45ba0"]//h2[@class="elementor-heading-title elementor-size-default"]//text()').extract_first(default="").strip()

                    dob = ""
                    placeofbirth = ""
                    country = ""
                    alias_name = ""
                    Identifications = ""
                    Associated = ""
                    gender = ""
                    try:
                        dob = ''.join(resp.xpath('//*[contains(text(),"Date of Birth:")]/../../..//*[@class="elementor-element elementor-element-9a896ea dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        dob = ""
                    try:
                        placeofbirth = ''.join(resp.xpath('//*[contains(text(),"Place of Birth:")]/../../..//*[@class="elementor-element elementor-element-89fe776 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        placeofbirth = ""
                    try:
                        country = ''.join(resp.xpath('//*[contains(text(),"Citizenship:")]/../../..//*[@class="elementor-element elementor-element-fcb86f5 dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"Nationality:")]/../../..//*[@class="elementor-element elementor-element-d94db6a dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        country = ""
                    try:
                        alias_name = ''.join(resp.xpath('//*[contains(text(),"Aliases/Alternative Name Spellings:")]/../../..//*[@class="elementor-element elementor-element-50c20df dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()).strip()
                    except:
                        alias_name = ""
                    try:
                        Identifications = resp.xpath('//*[contains(text(),"Miscellaneous Identifications:")]/../../..//*[@class="elementor-element elementor-element-c8cff6e dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()|//*[contains(text(),"National Identification Number(s) and Country:")]/../../..//*[@class="elementor-element elementor-element-4f362cf dc-has-condition dc-condition-empty elementor-widget elementor-widget-text-editor"]//text()').extract()
                    except:
                        Identifications = ""
                    try:
                        Associated = resp.xpath('//*[contains(text(),"Associated Organization(s):")]/../../..//a/text()').extract_first(default="").strip()
                    except:
                        Associated = ""
                    try:
                        gender = ''.join(resp.xpath('//*[contains(text(),"Sex:")]/../../..//*[@class="elementor-element elementor-element-11fd5fa elementor-widget elementor-widget-jet-listing-dynamic-terms"]//text()').extract()).strip()
                    except:
                        gender = ""
            
                    try:
                        comment = ''.join(resp.xpath('//*[@class="elementor-element elementor-element-52b1d20 elementor-widget elementor-widget-theme-post-content"]//p//text()').extract()).strip()
                    except:
                        comment = ""

                    item = {}
                    item['uid'] = self.get_hash(name)
                    item['name'] = name
                    item['alias_name'] = []
                    if len(alias_name.split(";"))>1:
                        item['alias_name'] = alias_name.split(";")
                    else:
                        item['alias_name'] = alias_name.split(",")
                    item['country'] = []
                    item['list_type'] = "Individual"
                    item['last_updated'] = self.last_updated_string
                    # item['Family_Tree'] = {}
                    # item['Family_Tree']['Associated'] = Associated
                    item['sanction_Details'] = {}
                    # item['sanction_Details']['Associated'] = Associated
                    item['individual_details'] = {}
                    item['individual_details']['date_of_birth'] = dob.split(";")
                    item['individual_details']['Gender'] = gender
                    item['individual_details']['Place_of_Birth'] = placeofbirth
                    item['nns_status'] = False
                    item['address'] = []
                    add = {}
                    # add['country'] = country.replace("\t","").replace("\n","").strip()
                    add['country'] = country.split("\t")[0]
                    add['complete_address'] = ""
                    item['address'].append(add)
                    item['documents'] = {}
                    # try:
                    #     body = [I.strip() for I in Identifications]
                    #     for H in body:
                    #         if H != "":
                    #             data = H.strip().replace("\n","").split(":")
                    #             item['documents'][data[0]] = data[1]
                    #         else:
                    #             continue
                    # except:
                    #     pass
                    item['comment'] = comment
                    item['sanction_list'] = {}
                    item['sanction_list']['sl_authority'] = "Rewards for Justice, USA"
                    item['sanction_list']['sl_url'] = "https://rewardsforjustice.net/index/"
                    item['sanction_list']['sl_host_country'] = "USA"
                    item['sanction_list']['sl_type'] = "Sanctions"
                    item['sanction_list']['watch_list'] = "North America Watchlists"
                    item['sanction_list']['sl_source'] = "Rewards for Justice-Wanted for Terrorism List, Russia"
                    item['sanction_list']['sl_description'] = "List of most wanted individuals and entities by Rewards for Justice, USA."
                    item['list_id'] = "USA_T30098"
                    self.out_list.append(item)                
            cp+=1
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
temp.UploadfilestTos3()
