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
            try:
                body = response.xpath('//*[@class="rightInnerContent floatRight"]//a/@href').extract()
                for i in body:
                    counter = counter + 1
                    link = "https://www.mca.gov.in" + i
                    res = requests.get(link, headers=headers, data=payload)
                    f = open(f"mca.gov.in_697_{counter}.pdf", "wb")
                    f.write(res.content)
                    f.close()
                    try:
                        tb = camelot.read_pdf(f"mca.gov.in_697_{counter}.pdf", pages="all", flavor="stream")
                        for i in tb:
                            df = i.df
                            # df = pd.DataFrame(df)
                            for row in df.iterrows():
                                try:
                                    signatory_id = row[1][0]
                                except:
                                    signatory_id = ""
                                try:
                                    name = row[1][1]
                                except:
                                    name = ""
                                try:
                                    cin = row[1][2]
                                except:
                                    cin = ""
                                try:
                                    company_name = row[1][3]
                                except:
                                    company_name = ""
                                try:
                                    default_year = row[1][4]
                                except:
                                    default_year = ""
                                if name != 'Name' and name != "":
                                    item = {}
                                    item['uid'] = hashlib.sha256(((name).lower()).encode()).hexdigest()
                                    item['name'] = name
                                    item['alias_name'] = []
                                    item['country'] = []
                                    item['list_type'] = "Individual"
                                    item['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                                    item['individual_details'] = {}
                                    item['sanction_details'] = {}
                                    item['sanction_details']['Defaulting_Year'] = default_year
                                    item['family_tree'] = {}
                                    item['family_tree']['Associate'] = company_name
                                    item['nns_status'] = False
                                    item['address'] = []
                                    item['documents'] = {}
                                    item['documents']['Signatory_ID'] = signatory_id
                                    item['comment'] = {"Cin":cin,"Company_name":company_name}
                                    item['sanction_list'] = {}
                                    item['sanction_list']['sl_authority'] = "Ministry of Corporate Affairs, India"
                                    item['sanction_list']['sl_url'] = "https://www.mca.gov.in/MinistryV2/defaultersecretarieslist.html"
                                    item['sanction_list']['sl_host_country'] = "India"
                                    item['sanction_list']['sl_type'] = "Sanctions"
                                    item['sanction_list']['watch_list'] = "India Watchlists"
                                    item['sanction_list']['sl_source'] = "Ministry of Corporate Affairs Defaulter Directors List, India"
                                    item['sanction_list']['sl_description'] = "List of defaulter Directors by Ministry of Corporate Affairs, India."
                                    item['sanction_list']['list_id'] = "IND_E20296"
                                    self.final_list.append(item)
                    except Exception as e:
                        print(e)
                try:
                    ab = json.dumps(self.final_list)
                    f = open(f"{self.outputpath}/{self.date}_output_www.mca.gov.in_697.json", "w")
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