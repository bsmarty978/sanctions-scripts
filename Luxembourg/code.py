import requests
import json
import re
import datetime
import pandas as pd
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time as t
import xml.etree.ElementTree as ET
import os
from bs4 import BeautifulSoup
import hashlib
import copy
import boto3
from os.path import exists
import unidecode

class europe():
    try:
        root = "/home/ubuntu/sanctions-scripts/Luxembourg/"
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        Final_list = []
        inputpath = f'{root}InputFiles'
        outputpath = f'{root}OutputFiles'
        Differencepath = f'{root}DifferenceFiles'
        rmpath = f"{root}RemovedFiles"
        lp_path = f"{root}eu_log.csv"
        last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
        os.mkdir(rmpath)
    except:
        pass

    def uploadToS3(self):
        try:
            print("uploading files to s3")
            s3 = boto3.client('s3')
            s3.upload_file(f'{self.inputpath}/{self.date}_input_eu.xml', "sams-scrapping-data",
                           f"EU/Original/{self.date}_input_eu.xml")
            s3.upload_file(f'{self.outputpath}/{self.date}_output_eu.json', "sams-scrapping-data",
                           f"EU/Parced/{self.date}_output_eu.json")
            s3.upload_file(f'{self.Differencepath}/{self.date}_Difference_Output_eu.json', "sams-scrapping-data",
                           f"EU/Diffrance/{self.date}_Difference_Output_eu.json")
            s3.upload_file(f'{self.rmpath}/{self.date}_RemovedData_File_eu.json', "sams-scrapping-data",
                           f"EU/Removed/{self.date}_RemovedData_File_eu.json")
            s3.upload_file(f'{self.lp_path}', "sams-scrapping-data",
                           f"EU/{self.lp_path}")
            print("uploaded files to s3")
        except Exception as e:
            print(e)

    def login(self):
        try:
            id_pass = ['joginderbairwa@gmail.com','8!M3kq9:w?fT4cj']
            url = "https://ecas.ec.europa.eu/cas/login?loginRequestId=ECAS_LR-16631896-cryAhSdBpxNzzRb4s3NGygnyazTg26FwVBCelTxzjLjLaDyXmIUefIWJzudOB4bDhwiqbsfscPCS4nYf01T3q8m-jpJZscgsw0KgSCWq5vTIaa-2XPVQS0fBQpzzhvIT57gymWFaB0Xh7tGzRqS5JYe9LCWyBE6w9qAzzOTsvkiJKI9GFO0YqywYTNptT7V7QqusHY"
            chrome_options = Options()
            # chrome_options.add_experimental_option("debuggerAddress", "localhost:9020")
            chrome_options.add_argument("--start-maximized")
            # chrome_options.add_argument('--headless')
            driver = webdriver.Chrome(chrome_options=chrome_options, executable_path="D:\\chromedriver.exe")
            driver.get(url)
            t.sleep(7)
            try:
                username = driver.find_element_by_xpath('//*[@name="username"]')
                username.clear()
                t.sleep(1)
                username.send_keys(f"{id_pass[0]}")
            except Exception as e:
                print(e)
            try:
                next_button = driver.find_element_by_xpath('//*[@name="whoamiSubmit"]').click()
            except Exception as e:
                print(e)
            try:
                t.sleep(2)
                enter_password = driver.find_element_by_xpath('//*[@name="password"]')
                enter_password.clear()
                t.sleep(1)
                enter_password.send_keys(f"{id_pass[-1]}")
            except Exception as e:
                print(e)
            try:
                sign_in = driver.find_element_by_xpath('//*[@name="_submit"]').click()
            except Exception as e:
                print(e)

            try:
                # t.sleep(10)
                driver.execute_script("window.open('https://webgate.ec.europa.eu/fsd/fsf');")
                t.sleep(5)
                res = driver.page_source
                response = HtmlResponse(url="example.com",body=res,encoding="utf-8")
                try:
                    link = "https://webgate.ec.europa.eu/fsd/fsf/" + response.xpath('//*[@class="row files-row file-row file-box ng-scope"]//*[@title="Consolidated Sanctions List based on the XML Schema Definition (XSD)"]//@href').extract_first(default="").strip()
                except:
                    link = ""

                ress = requests.get(link)
                df = open(f"{self.inputpath}/{self.date}_input_eu.xml", 'wb')
                df.write(ress.content)
                driver.close()
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

    def xmlfiledownloder(self):
        URL = "https://data.opensanctions.org/datasets/latest/eu_fsf/source.xml"
        response = requests.get(URL)
        try:
            with open(f"{self.inputpath}/{self.date}_input_eu.xml", 'wb') as file:
                file.write(response.content)
        except FileNotFoundError:
            os.mkdir(self.inputpath)
            with open(f"{self.inputpath}/{self.date}_input_eu.xml", 'wb') as file:
                file.write(response.content)


    def compare(self):
        try:
            with open(f'{self.outputpath}/{self.yesterday}_output_eu.json', 'rb') as f:
                data = f.read()
        except:
            # print("---------------------Alert--------------------------")
            print(f"There is not any old file for date: {self.yesterday.ctime()}")
            # print("----------------------------------------------------")
            data = "No DATA"
        
        old_list = []
        if data != "No DATA":
            old_list = json.loads(data)
        new_list = copy.deepcopy(self.Final_list)

        new_profiles = []
        removed_profiles = []
        updated_profiles = []

        old_dict = {}
        for val in old_list:
            old_dict[val["uid"]] = val

        new_uid_list = []
        for val1 in new_list:
            new_uid = val1["uid"]
            new_uid_list.append(new_uid)
            if new_uid in old_dict.keys():
                # print("Already in List")
                for i in val1:
                    if i != "last_updated":
                        try:
                            if val1[i] != old_dict[val1["uid"]][i]:
                                print(f"Updataion Detected on: {val1['uid']} for: {i}")
                                updated_profiles.append(val1)
                                break
                        except:
                            print(f"Updataion Detected on: {val1['uid']} for: {i}")
                            updated_profiles.append(val1)
                            break


            else:
                new_profiles.append(val1)
                print(f"New Profile Detected : {val1['uid']}")

        for val2 in old_dict.keys():
            if val2 not in new_uid_list:
                print(f"Removed Profile Detected : {val2}")
                removed_profiles.append(old_dict[val2])

        if len(new_list)==0:
            removed_profiles = []
            raise ValueError("Data Parsing Error Fix it Quickly ⚒️")
        print("------------------------LOG-DATA---------------------------")
        print(f"total New Profiles Detected:     {len(new_profiles)}")
        print(f"total Updated Profiles Detected: {len(updated_profiles)}")
        print(f"total Removed Profiles Detected: {len(removed_profiles)}")
        print("-----------------------------------------------------------")
        #NOTE: ---------- META LOg FIlE --------------------------
        try:
            with open('/home/ubuntu/meta-scrap-log.json', 'rb') as f:
                logd = f.read()
                pred = json.loads(logd)
                pred.append({
                    "dag" : "EU",
                    "date" : self.last_updated_string,
                    "total" : len(new_list),
                    "new" :  len(new_profiles),
                    "update" : len(updated_profiles),
                    "remove" : len(removed_profiles),
                })
            with open('/home/ubuntu/meta-scrap-log.json', 'w') as f:
                json.dump(pred,f)
        except:
            print("ERROR WHILE UPDATING META LOG FILE")
        #NOTE:  -----------------------------------------------------


        with open(f'{self.Differencepath}/{self.date}_Difference_Output_eu.json', "w") as outfile:
            json.dump(new_profiles + updated_profiles, outfile)

        with open(f'{self.rmpath}/{self.date}_RemovedData_File_eu.json', "w") as outfile:
            json.dump(removed_profiles, outfile)

        if exists(self.lp_path):
            with open(f'{self.lp_path}',"a") as outfile:
                passing = f"{self.last_updated_string},{self.date}_input_eu.xml,{self.date}_output_eu.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_eu.json',{self.date}_removed_eu.json'\n"
                outfile.write(passing)
        else:
            with open(f'{self.lp_path}',"a") as outfile:
                pass_first = "date,inputfile,outputfile,total_profile_scraped,new,updated,diffrance,removed,diffrancefile,removedfile\n"
                passing = f"{self.last_updated_string},{self.date}_input_eu.xml,{self.date}_output_eu.json',{len(new_list)},{len(new_profiles)},{len(updated_profiles)},{len(new_profiles)+len(updated_profiles)},{len(removed_profiles)},{self.date}_diffrance_eu.json',{self.date}_removed_eu.json'\n"
                outfile.write(pass_first)
                outfile.write(passing)

    def alias_gen(self,nmstr):
        out = []
        nm = nmstr.split(' ')
        if len(nm)==2:
            out.append(nm[1].strip() + " " + nm[0].strip())
        if len(nm) > 2:
            out.append(nm[0].strip() + " " + nm[2].strip())
            out.append(nm[2].strip() + " " + nm[0].strip())
            out.append(nm[1].strip() + " " + nm[2].strip())
            out.append(nm[2].strip() + " " + nm[1].strip())
        else:
            out.append(nmstr)
        return out

    def isEnglish(self,s):
        try:
            s.encode(encoding='utf-8').decode('ascii')
        except UnicodeDecodeError:
            return False
        else:
            return True


    def parsing(self):
        iden = ""
        key = ""
        try:
            last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            # f = open(f"{self.inputpath}/{self.date}_input_eu.xml", "r", encoding="utf8")
            # f = open(f"20220211-FULL-1_1(xsd).xml", "r", encoding="utf8")
            f = open(f"{self.inputpath}/{self.date}_input_eu.xml", "r", encoding="utf8")
            body = f.read()
            data = BeautifulSoup(body, "xml")
            for i in data.find_all("sanctionEntity"):
                key = ""
                value = ""
                item = {}
                for jj in i.find_all('subjectType'):
                    type = jj.get('code')
                alias_name = []
                euReferenceNumber = i.get('euReferenceNumber')
                for reg in i.find_all('regulation'):
                    numberTitle = reg.get('numberTitle')
                    programe = reg.get('programme')
                for j in i.find_all("remark"):
                    remark = j.text
                for otherurl in i.find_all('publicationUrl'):
                    otherurl = otherurl.text
                counter = 0
                for name in i.find_all('nameAlias'):
                    if counter == 0:
                        Fullname = name.get('wholeName')
                        counter = counter + 1
                    else:
                        alis_name = name.get('wholeName')
                        alias_name.append(alis_name)
                for add in i.find_all('citizenship'):
                    country = add.get('countryDescription')
                    coun2dig = add.get('countryIso2Code')
                    pubdate = add.get('publicationDate')
                    numtitle = add.get('numberTitle')
                for birth in i.find_all('birthdate'):
                    caltype = birth.get('calendarType')
                    city = birth.get('city')
                    zipcode = birth.get('zipCode')
                    birthdate = birth.get('birthdate')
                for iden in i.find_all('identification'):
                    try:
                        docs = iden.get('identificationTypeCode')
                        if docs == "passport":
                            key = iden.get('identificationTypeCode')
                            value = iden.get('number')
                    except:
                        docs = ""
                        value = ""
                        key = ""
                uid = Fullname + str(birthdate) + "European Union Consolidated Sanctions List"
                # uid = i.gte("logicalId")
                item['uid'] = hashlib.sha256(((uid + "eu Sanctions").lower()).encode()).hexdigest()
                if not self.isEnglish(Fullname):
                    Fullname = unidecode.unidecode(Fullname)

                item['name'] = Fullname
                item['alias_name'] = []
                item['alias_name'].append(' '.join(alias_name))

                if item['alias_name'] == [] or item['alias_name'] == [""]:
                    item['alias_name'] = self.alias_gen(Fullname)

                item['country'] = []
                item['country'].append(country)
                if type == "person":
                    item['list_type'] = "Individual"
                    item['last_updated'] = last_updated_string
                    item['sanction_details'] = {}
                    item['sanction_details']['euReferenceNumber'] = euReferenceNumber
                    item['sanction_details']['numberTitle'] = numberTitle
                    item['sanction_details']['programme'] = programe
                    item['individual_details'] = {}
                    item['individual_details']['Nation'] = country
                    item['individual_details']['date_of_birth'] = []
                    if birthdate != None or "":
                        item['individual_details']['date_of_birth'].append(birthdate)
                    item['nns_status'] = False
                    if key != "":
                        item['documents'] = {}
                        item['documents'][key] = value
                    else:
                        item['documents'] = {}
                    item['address'] = []
                    address = {}
                    address['complete_address'] = city + zipcode + country
                    address['country'] = country
                    address['zip'] = zipcode
                    address['city'] = city
                    item['address'].append(address)
                    item['comment'] = remark
                    item['sanction_list'] = {
                        "sl_authority": "Commission De Surveillance Du Secteur Financier, Luxembourg",
                        "sl_url": "https://www.cssf.lu/en/Document/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions/",
                        "sl_host_country": "Luxembourg",
                        "sl_type": "Sanctions",
                        "watch_list": "European Watchlists",
                        "sl_source": "Commission De Surveillance Du Secteur Financier Sanctions List, Luxembourg",
                        "sl_description": "Sanctioned imposed by The Commission De Surveillance Du Secteur Financier, Luxembourg."
                        }
                    item['list_id'] = "LUX_E20196"
                    self.Final_list.append(item)
                elif type == "enterprise":
                    item['list_type'] = "Entity"
                    item['last_updated'] = last_updated_string
                    item['sanction_details'] = {}
                    item['sanction_details']['euReferenceNumber'] = euReferenceNumber
                    item['sanction_details']['numberTitle'] = numberTitle
                    item['sanction_details']['programme'] = programe
                    item['entity_details'] = {}
                    item['nns_status'] = False
                    if key != "":
                        item['documents'] = {}
                        item['documents'][key] = iden.get('number')
                    else:
                        item['documents'] = {}
                    item['address'] = []
                    address = {}
                    address['complete_address'] = city + zipcode + country
                    address['country'] = country
                    address['zip'] = zipcode
                    address['city'] = city
                    item['address'].append(address)
                    item['comment'] = remark
                    item['sanction_list'] = {
                        "sl_authority": "Commission De Surveillance Du Secteur Financier, Luxembourg",
                        "sl_url": "https://www.cssf.lu/en/Document/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions/",
                        "sl_host_country": "Luxembourg",
                        "sl_type": "Sanctions",
                        "watch_list": "European Watchlists",
                        "sl_source": "Commission De Surveillance Du Secteur Financier Sanctions List, Luxembourg",
                        "sl_description": "Sanctioned imposed by The Commission De Surveillance Du Secteur Financier, Luxembourg."
                        }
                    item['list_id'] = "LUX_E20196"
                    self.Final_list.append(item)
            try:
                ab = json.dumps(self.Final_list)
                f = open(f"{self.outputpath}/{self.date}_output_eu.json", "w",encoding='UTF-8')
                f.write(ab)
                f.close()
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
        if self.Final_list ==[]:
            raise ValueError("Error : Data Parsing Error.... fix it quickly ⚒️")

        

temp = europe()
# temp.login()
temp.xmlfiledownloder()
temp.parsing()
temp.compare()
temp.uploadToS3()