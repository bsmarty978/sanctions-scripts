from ast import Break
from cv2 import ml_ANN_MLP
import requests
import json
import string
from string import punctuation
from datetime import datetime,date,timedelta
today_date  = date.today()
import time
import pymongo
from copy import deepcopy




# yesterday = today_date - timedelta(days = 1)
# l = list(string.ascii_lowercase)
# ml = []
# for i in l:
#     for k in l:
#         for j in l:
#             ml.append(i+k+j)
# d_l = []
# for i1 in range(10):
#     for i2 in range(10):
#         for i3 in range(10):
#             d_l.append(f"{i1}{i2}{i3}")


l = list(string.ascii_lowercase) + [str(num) for num in list(range(10))]  + list(punctuation)
# l = list(string.ascii_lowercase) + [str(num) for num in list(range(10))]  + ['"',"'","(",")","[","]",".",":","&","@","+"]
ml = [] 
for i in l:
    for k in l:
        for j in l:
            ml.append(i+k+j)

print(f"Total Combination : {len(ml)}")
# exit()
# time.sleep(20)
# print("BOOM")
# exit()

# ml.extend(d_l)


#NOTE:MONGO-DB 
client = pymongo.MongoClient("mongodb+srv://smt_mca_admin:smtMCA%40886611@mca-cluster.kdxoh.mongodb.net/?retryWrites=true&w=majority")
db = client["MCA-MASTER-DATA"]


def mongo_push(dataobj):
    collec = db.mcacin
    collec.insert_many(dataobj)
#-----------------------------------------


url = f"https://www.mca.gov.in/mcafoportal/cinLookup.do"
main_out_list = []
failed_names = []

root = "/home/ubuntu/sanctions-scripts/MCA-ALL/"
c = 0
cp = True
for d in ml:
    # if d=="uy8":
    #     cp = False
    # if cp:
    #     continue
    payload = {"companyname": d}
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        "authority": "www.mca.gov.in",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    res = requests.post(url, headers=headers, data=payload)
    try:
        data = json.loads(res.text)["companyList"]
        main_out_list.extend(data)
        try:
            passdata = deepcopy(data)
            mongo_push(passdata)
        except Exception as e:
            print(f"-->mongo Error")
            print(f"-->{e}")
    except Exception as e:
        print(f"ERROR FOR : {d}")
        failed_names.append(d)
        print(f"-->{e}")
    if c == 1:
        break
    c+=1


with open(f"{root}mca-ds-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(main_out_list, outfile, ensure_ascii=False, indent=4)

with open(f"{root}failed_names-ds-nohup-{today_date}.json", "w",encoding='utf-8') as outfile:
    json.dump(failed_names, outfile, ensure_ascii=False, indent=4)