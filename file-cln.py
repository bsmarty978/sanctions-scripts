import os
from os.path import exists
from datetime import date,datetime,timedelta

today_date  = date.today()


#NOTE: CONCEPT
yesterday = today_date - timedelta(days = 30)
old_date = f'{yesterday.day}-{yesterday.month}-{yesterday.year}'
old_month = f'{yesterday.month}-{yesterday.year}'

del_dates1 = []
del_dates2 = []
for k in range(10,100):
    ys = today_date - timedelta(days=k)
    del_dates2.append(ys)
    del_dates1.append(f'{ys.day}-{ys.month}-{ys.year}')


base_dir = "/home/ubuntu/sanctions-scripts"
del_fl1 = ["outputfiles","inputfiles","removedfiles","diffrancefiles"]
del_fl2 = ["OutputFiles","InputFiles","RemovedFiles","DifferenceFiles"]
c = 0
for i in os.listdir(base_dir):
    try:
        if os.path.isdir(f"{base_dir}/{i}"):
            print(f"---\n-->{i}")
            # c+=1
            for k in del_fl1:
                if k in os.listdir(f"{base_dir}/{i}"):
                    for del_d in del_dates1:
                        for f in os.listdir(f"{base_dir}/{i}/{k}"):
                            if f"{del_d}" in f:
                                os.remove(f"{base_dir}/{i}/{k}/{f}")
                                # print(f"{base_dir}/{i}/{k}/{f}")
                                c+=1
                                # print(f) 
            for k in del_fl2:
                if k in os.listdir(f"{base_dir}/{i}"):
                    for del_d in del_dates2:
                        for f in os.listdir(f"{base_dir}/{i}/{k}"):
                            if f"{del_d}" in f:
                                os.remove(f"{base_dir}/{i}/{k}/{f}")
                                # print(f"{base_dir}/{i}/{k}/{f}")
                                c+=1
                                # print(f) 
                # print("-----")
    except Exception as e:
        print(e)
        break
print(c)