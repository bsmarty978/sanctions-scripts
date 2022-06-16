import requests
import json
from datetime import date,datetime,timedelta
import traceback

today_date  = date.today()
root = "/home/ubuntu/sanctions-scripts/FINOVO/"

out_list = []
failed_cin = []

failed_c = 0

cdata = json.load(open("/home/ubuntu/sanctions-scripts/MCA-ALL/mca-all-ds-2022-05-30.json"))
n = 100000
for i in range(n):
    cin =  cdata[i]["companyID"]
    cname = cdata[i]["companyName"]
    url = f"https://api.finanvo.in/company/profile?CIN={cin}"
    print(f"-->{cname}")
    try:
        r =  requests.get(url)

        if r.status_code==200:
            data = json.loads(r.text)
            out_list.append({
                "cin" : data["data"]["CIN"],
                "name" : data["data"]["COMPANY_NAME"],
                "status" : data["data"]["COMPANY_STATUS"],
                "class" : data["data"]["CLASS"],
                "catrgory" :data["data"]["CATEGORY"],
                "address" : data["data"]["REGISTERED_OFFICE_ADDRESS"]
            }
            )
        else:
            failed_c += 1
            failed_cin.append(cin)
    except:
        print(("fff"))
        # print(traceback.print_exc())
        failed_c += 1
        failed_cin.append(cin)
        # break

print("*-*-*-*--*-*-*-*-*-*-*-*-*")
print(f"Total CIN : {n}")
print(f"Failed CIN : {len(failed_cin)}")
print("*-*-*-*--*-*-*-*-*-*-*-*-*")

with open(f"{root}finvo-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(out_list, outfile, ensure_ascii=False, indent=4)

with open(f"{root}finvo-failed-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(failed_cin, outfile, ensure_ascii=False, indent=4)


