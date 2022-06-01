import requests
import json
from scrapy.http import HtmlResponse
import datetime
import os
import pandas as pd
import time as t
import boto3
import copy
import hashlib


class Data():
    try:
        final_list = []
        date = datetime.date.today()
        yesterday = date - datetime.timedelta(days=1)
        inputpath = f'InputFiles'
        outputpath = f'OutputFiles'
        Differencepath = f'DifferenceFiles'
        os.mkdir(inputpath)
        os.mkdir(outputpath)
        os.mkdir(Differencepath)
    except:
        pass

    def parsing(self):
        try:
            df = pd.read_excel('Defaulting_clients.xlsx',sheet_name='NSE')
            print(df)

            for i in range(len(df)):
                try:
                    name = df.iloc[i, 1]
                except:
                    name = ""
                try:
                    pan = df.iloc[i, 2]
                except:
                    pan = ''
                try:
                    Appellate_Matter_No = df.iloc[i, 4]
                except:
                    Appellate_Matter_No = ''
                try:
                    Award1 = df.iloc[i, 5]
                    converted_num = str(Award1)
                    Award = converted_num.replace(' 00:00:00','')
                except Exception as e:
                    print(e)
                    Award = ''
                try:
                    comment = ""
                    comment = df.iloc[i, 6]
                except:
                    comment = ""


                item = {}
                last_updated_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                item = {}
                item['uid'] = hashlib.sha256(((name + "NSE Regulatory Defaulting Clients, India IND_E20310").lower()).encode()).hexdigest()
                item['name'] = name.strip()
                item['alias_name'] = []
                item['country'] = []
                item['list_type'] = "Individual"
                item['last_updated'] = last_updated_string
                item['individual_details'] = {}
                # item['entity_details']['website'] = []
                item['nns_status'] = False
                item['address'] = []
                add = {}
                add['complete_address'] = ""
                add['country'] = ""
                item['address'].append(add)
                item['sanction_details'] = {}
                item['sanction_details']['body'] = Appellate_Matter_No
                item['sanction_details']['date_of_order'] = Award
                item['documents'] = {}
                item['documents']['PAN'] = [pan]
                item['comment'] = comment
                item['sanction_list'] = {}
                item['sanction_list']['sl_authority'] = "NSE Regulatory Defaulting Clients, India"
                item['sanction_list']['sl_url'] = "https://www.bseindia.com/"
                item['sanction_list']['sl_host_country'] = "India"
                item['sanction_list']['sl_type'] = "Sanctions"
                item['sanction_list'][
                    'sl_source'] = "NSE Regulatory Defaulting Clients, India"
                item['sanction_list'][
                    'sl_description'] = "NSE Regulatory Defaulting Clients, India"
                item['sanction_list']['watch_list'] = "India Watchlists"
                item['sanction_list']['list_id'] = "IND_E20310"
                self.final_list.append(item)
        except Exception as e:
            print(e)
        try:
            with open(f'{self.outputpath}/{self.date}_output_sng-795-NSE-Regulatory-Defaulting-Clients.ch.json', "w") as outfile:
                json.dump(self.final_list, outfile, indent=4)

        except Exception as e:
            print(e)
        except Exception as e:
            print(e)


temp = Data()
temp.parsing()
# temp.compare()
# temp.uploadToS3()
