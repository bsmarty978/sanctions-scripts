import requests
import json
import string

l = list(string.ascii_lowercase)
ml = []
for i in l:
    for k in l:
        for j in l:
            ml.append(i+k+j)


url = f"https://www.mca.gov.in/mcafoportal/cinLookup.do"
main_out_list = []
failed_names = []

root = "/home/ubuntu/sanctions-scripts/MCA-ALL/"
c = 0
for d in ml:
    # if c == 10:
    #     break
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
    except Exception as e:
        print(f"ERROR FOR : {d}")
        failed_names.append(d)
        print(f"-->{e}")
    # c+=1

with open(f"{root}mca-all.json", "w",encoding='utf-8') as outfile:
    json.dump(main_out_list, outfile, ensure_ascii=False, indent=4)

with open(f"{root}failed_names.json", "w",encoding='utf-8') as outfile:
    json.dump(failed_names, outfile, ensure_ascii=False, indent=4)