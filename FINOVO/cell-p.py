
import json
import pandas as pd
from string import punctuation
from datetime import datetime,date,timedelta
import hashlib

today_date  = date.today()
dag_name = "mcx-struckoff"
root = "/home/ubuntu/sanctions-scripts/FINOVO/"


pu = list(punctuation)
last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

main_df = pd.read_excel("/home/ubuntu/sanctions-scripts/FINOVO/MCA.xlsx")
main_df.fillna("",inplace=True)

def get_hash(n):
    return hashlib.sha256(((n+ "MCA Shell (SEBI)").lower()).encode()).hexdigest()

mc = 0
out_list = []
for i,r in main_df.iterrows():
    pcin = r["CIN"]
    cname = r["Company Name"]
    cname = str(cname).replace("\n"," ").strip()
    if not cname:
        continue
    pc = True
    for p in pu:
        if p in str(pcin):
            pc=False
    if pc:
        mc +=1
        print(f"{pcin} -> {cname}")
        d = {
            "uid": get_hash(cname+str(pcin)),
            "name": cname,
            "alias_name": [],
            "country": ["India"],
            "list_type": "Entity",
            "entity_details" : {},
            "last_updated": last_updated_string,
            "nns_status": "False",
            "address": [
                {
                "country": "",
                "complete_address": ""
                }
            ],
            "sanction_details":{},
            "documents": {
                "CIN" : str(pcin)
            },
            "comment": "",
            "sanction_list" : {
                "sl_authority" : "Securities and Exchange Board of India(SEBI)",
                "sl_url" : "https://www.sebi.gov.in/",
                "watch_list" : "India Watchlists",
                "sl_host_country" : "India",
                "sl_type" : "Shell corporation",
                "sl_source" : "Shell (SEBI)",
                "sl_description" : "Securities and Exchange Board of India (SEBI) is a statutory regulatory body entrusted with the responsibility to regulate the Indian capital markets. It monitors and regulates the securities market and protects the interests of the investors by enforcing certain rules and regulations.SEBI was founded on April 12, 1992, under the SEBI Act, 1992. Headquartered in Mumbai, India, SEBI has regional offices in New Delhi, Chennai, Kolkata and Ahmedabad along with other local regional offices across prominent cities in India.The objective of SEBI is to ensure that the Indian capital market works in a systematic manner and provide investors with a transparent environment for their investment. To put it simply, the primary reason for setting up SEBI was to prevent malpractices in the capital market of India and promote the development of the capital markets."
            }
        }
        out_list.append(d)
    if mc >= 300000:
        break


with open(f"{root}MCX-struck-off-1-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(out_list[0:100000], outfile, ensure_ascii=False, indent=4)
with open(f"{root}MCX-struck-off-2-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(out_list[100000:200000], outfile, ensure_ascii=False, indent=4)
with open(f"{root}MCX-struck-off-3-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(out_list[200000:299999], outfile, ensure_ascii=False, indent=4)

