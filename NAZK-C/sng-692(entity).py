import json
from urllib import response
import requests
import hashlib
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
import pandas as pd
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


url = "https://sanctions.nazk.gov.ua/en/api/company/"

file = pd.read_csv("E:\HM treasury_uk\company.csv")


data = requests.get(url)
#print(data)

textdata = json.loads(data.text)
print(len(textdata["data"]))

mylist = []

for i in textdata["data"]:
    d = {"uid": "",
                    "name": "",
                    "alias_name": [],
                    "country": [],
                    "list_type": "Entity",
                    "last_updated": last_updated_string,
                    "list_id": "UKR_S10049",
                    "nns_status": "False",
                    "address": [
                    {
                    "country": "",
                    "complete_address": ""
                    }
                    ],
                    "Entity_details": {
                        "TIN":"",
                        "SRN":""
                    },
                    "documents": {
                    },
                    "comment": "",
                    "sanction_list": {
                    "sl_authority": "National Agency on Corruption Prevention, Ministry of Foreign Affairs, Ukraine",
                    "sl_url": "https://sanctions.nazk.gov.ua/en/sanction-company/",
                    "watch_list": "European Watchlists",
                    "sl_host_country": "Ukraine",
                    "sl_type": "Sanctions",
                    "sl_source": "Ministry of Foreign Affairs, National Agency on Corruption Prevention Sanctions List, Ukraine",
                    "sl_description": "List of individuals and entities sanctioned by National Agency on Corruption Prevention, Ministry of Foreign Affairs, Ukraine"

                    }

    }
                    
    try:
        name = i["name"]
        if name!="":
            d["name"] = name
        #print("00000",title)
    except:
        #print("hi")
        pass
    try:
        country = file[file["Name"]==name].Country
        #print(country)

        d["country"].append(country.iloc[0])

        

    except:
        pass  
    # try:
    #     link = "https://www.mas.gov.sg"+i["page_url_s"]
    #     d["URL"] = link
    #     webpage = requests.get(link)

    #     soup = BeautifulSoup(webpage.content, "html.parser")

    #     dom = etree.HTML(str(soup))
    #     content_l = []
    #     for j in dom.xpath("//div[@class='_mas-typeset contain m-t:l m-b:3xl mas-rte-content']//text()"):
    #         if j.strip():
    #             content_l.append(j.strip().replace("\xa0\xa0",""))
    #     content = "".join(content_l)
    #     d["Content"] = content
    #     #print(content)

    # except:
    #     pass


    try:
        tin = i["inn"]
        if tin!="":
            if "(" in tin:
                splittedtin = tin.split("(")
                #print(splittedtin)

                d["Entity_details"]["TIN"] = splittedtin[0]
            else:
                d["Entity_details"]["TIN"] = tin


    except:
        pass

    try:
        srn = i["ogrn"]
        if srn!="":
            d["Entity_details"]["SRN"] = srn

    except:
        pass


    try:
        com = i["reasoning_en"]
        if com!="":
            d["comment"]=com

    except:
        pass

    try:

        d["uid"] = hashlib.sha256(
            ((d["name"] + d["sanction_list"]["sl_source"]+d["sanction_list"]["sl_host_country"]).lower()).encode()).hexdigest()

    except:
        pass

    try:
        mylist.append(d)
        #print(d)
    except:
        pass    

with open('w1.json', 'w', encoding="utf-8") as file:
   json.dump(mylist, file, ensure_ascii=False, indent=4)