import os
from os.path import exists
from datetime import date,datetime,timedelta

today_date  = date.today()
yesterday = today_date - timedelta(days = 30)
old_date = f'{yesterday.day}-{yesterday.month}-{yesterday.year}'
old_month = f'{yesterday.month}-{yesterday.year}'
# old_date

base_dir = "/home/ubuntu/sanctions-scripts"
len(os.listdir(base_dir))
# del_dl1 = ["outputfiles","inputfiles","removedfiles","diffrancefiles"]
# del_dl2 = ["OutputFiles","InputFiles","RemovedFiles","DifferenceFiles"]
del_d1 = "diffrancefiles"
del_d2 = "DifferenceFiles"
c = 0
for i in os.listdir(base_dir):
    try:
        if os.path.isdir(f"{base_dir}/{i}"):
            print(f"---\n-->{i}")
            if del_d1 in os.listdir(f"{base_dir}/{i}"):
                    for j in os.listdir(f"{base_dir}/{i}/{del_d1}"):
                        if old_month in j:
                            c+=1
                            print(j)
                            os.remove(f"{base_dir}/{i}/{del_d1}/{j}")
                    # print("-----")1
            elif del_d2 in os.listdir(f"{base_dir}/{i}"):
                for j in os.listdir(f"{base_dir}/{i}/{del_d2}"):
                    if old_month in j:
                        c+=1
                        print(j)
                        os.remove(f"{base_dir}/{i}/{del_d2}/{j}")
                # print("-----")
    except Exception as e:
        print(e)
        break
print(c)
