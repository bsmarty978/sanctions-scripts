import pymongo
client = pymongo.MongoClient("mongodb+srv://smt_mca_admin:smtMCA%40886611@mca-cluster.kdxoh.mongodb.net/?retryWrites=true&w=majority")
db = client["MCA-MASTER-DATA"]

c = 0
cp =  True
for i in db.mcacin.find({"$expr": { "$lt": [{ "$strLenCP": '$companyID' }, 9] }}):
    # if c >10:
    #     break
    # c+=1
    cid = i["companyID"]
    if cid == "AAF-8056":
        cp= False
    if cp:
        continue
    # if len(cid)>7:
    #     continue
    dup_list = []
    for j in db.mcacin.find({"companyID":cid}):
        dup_list.append(j)
    total_dups = len(dup_list)
    print(cid,total_dups)
    if total_dups > 1:
        for d in range(total_dups-1):
            db.mcacin.delete_one({"companyID":cid})