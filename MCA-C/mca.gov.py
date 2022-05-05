import requests
from scrapy.http import HtmlResponse
import camelot
import pandas as pd
import datetime
import os
import json
import hashlib

class mcagov():
    try:
        counter = 0
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
        counter = 0
        try:
            url = "https://www.mca.gov.in/MinistryV2/defaultercompanieslist.html"

            payload = {}
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
            }

            ress = requests.get(url, headers=headers, data=payload)
            responsee = HtmlResponse(url="example.com",body=ress.content)
            try:
                body = responsee.xpath('//*[@class="resposive_tablet3"]//a/@href').extract()
                for i in body:
                    counter = counter + 1
                    link = "https://www.mca.gov.in" + i
                    res = requests.get(link,headers=headers,data=payload)
                    f = open(f"mca.gov.in_{counter}.pdf","wb")
                    f.write(res.content)
                    f.close()
                    try:
                        tb = camelot.read_pdf(f"mca.gov.in_{counter}.pdf", pages="all", flavor="stream")
                        df = tb[0].df
                        df = pd.DataFrame(df)
                        for row in df.iterrows():
                            try:
                                cin = row[1][0]
                            except:
                                cin = ""

                            try:
                                name = row[1][1]
                            except:
                                name = ""
                            if cin == "" and name == "Company Name":
                                continue
                            else:
                                item = {}
                                item['uid'] = hashlib.sha256(((name).lower()).encode()).hexdigest()
                                item['name'] = name.strip(".")
                                item['alias_name'] = []
                                item['country'] = []
                                item['list_type'] = "Entity"
                                item['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                                item['entity_details'] = {}
                                item['entity_details']['CIN'] = cin
                                item['nns_status'] = False
                                item['address'] = []
                                item['documents'] = {}
                                item['comment'] = {}
                                item['sanction_list'] = {}
                                item['sanction_list']['sl_authority'] = "Ministry of Corporate Affairs, India"
                                item['sanction_list']['sl_url'] = "https://www.mca.gov.in/MinistryV2/defaultercompanieslist.html"
                                item['sanction_list']['sl_host_country'] = "India"
                                item['sanction_list']['sl_type'] = "Sanctions"
                                item['sanction_list']['watch_list'] = "India Watchlists"
                                item['sanction_list']['sl_source'] = "Ministry of Corporate Affairs Defaulter Companies List, India"
                                item['sanction_list']['sl_description'] = "List of defaulter companies by Ministry of Corporate Affairs, India."
                                item['list_id'] = "IND_E20001"
                                self.final_list.append(item)
                    except Exception as e:
                        print(e)
                try:
                    ab = json.dumps(self.final_list)
                    f = open(f"{self.outputpath}/{self.date}_output_www.mca.gov.in.json", "a")
                    f.write(ab)
                    f.close()
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

temp = mcagov()
temp.parsing()