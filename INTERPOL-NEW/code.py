from bs4 import BeautifulSoup
from lxml import etree
import requests
import json

URL = "https://www.interpol.int/How-we-work/Notices/View-Red-Notices"

webpage = requests.get(URL)
soup = BeautifulSoup(webpage.content, "html.parser")
dom = etree.HTML(str(soup))
# print(dom.xpath('//*[@id="firstHeading"]')[0].text)
country_para = {}
for i in dom.xpath("//select[@id='nationality']/option"):
    if i.values():
        country_para[i.values()[0]] = i.text


def datascraper(passobj):
    ret_list = []
    for k in passobj["_embedded"]["notices"]:
        eid = k["entity_id"]
        # selflink = f"https://ws-public.interpol.int/notices/v1/red/{eid}"
        selflink = k["_links"]["self"]["href"]
        
        forename = k["forename"] if k["forename"] else ""
        name = k["name"] if k["name"] else ""
        name = (name + forename).strip()
        
        alias = []
        if forename and name:
            alias.append(forename+name)
            
        dob = [k["date_of_birth"]] if k["date_of_birth"] else []
        nationalities = k["nationalities"] if k["nationalities"] else []
        contry = []
        for c in nationalities:
            try:
                contry.append(country_para[c])
            except:
                print(f"conyry Error for :{c}")
                pass
                
        ret_list.append({
            "uid" : eid,
            "name" : name,
            "alias_name" : alias,
            "country" : contry,
            "individual_details":{
                "date_of_birth" : dob,
            },
        })

    return ret_list


def datascraper1(passobj):
    ret_list = []
    for k in passobj["_embedded"]["notices"]:
        eid = k["entity_id"]
        # selflink = f"https://ws-public.interpol.int/notices/v1/red/{eid}"
        selflink = k["_links"]["self"]["href"]
        # print(selflink)

        sr = requests.get(selflink)
        sdata = json.loads(sr.text)

        forename = sdata["forename"] if sdata["forename"] else ""
        name = sdata["name"] if sdata["name"] else ""
        name = (name + forename).strip()
        
        alias = []
        if forename and name:
            alias.append(forename+name)
        
        gender = sdata["sex_id"] if sdata["sex_id"] else ""
        if gender == "M":
            gender = "male"
        elif gender == "F":
            gender = "female"
        else:
            gender = ""

        dob = [sdata["date_of_birth"]] if sdata["date_of_birth"] else []

        pob = sdata["place_of_birth"] if sdata["place_of_birth"] else ""

        nationalities = sdata["nationalities"] if sdata["nationalities"] else []
        contry = []
        for c in nationalities:
            try:
                contry.append(country_para[c])
            except:
                print(f"conyry Error for :{c}")
                pass
                
        ret_list.append({
            "uid" : eid,
            "name" : name,
            "alias_name" : alias,
            "country" : contry,
            "individual_details":{
                "date_of_birth" : dob,
                "place_of_birth":pob,
                "gender" : gender
            },
            "dob" : dob,
        })

    return ret_list


mainc = 0
out_list = []
for rl in range(15,121):
    b_agr_url = f"https://ws-public.interpol.int/notices/v1/red?&ageMin={rl}&ageMax={rl}&resultPerPage=160"
    nr = requests.get(b_agr_url)
    print(nr)
    data = json.loads(nr.text)
    t_data = data["total"]
    print(f"{rl}-->{t_data}")
    mainc+=t_data
    datac = 0
    if t_data > 160:
        if datac >= t_data:
            break
        for k in country_para.keys():
            b_agrn_url = f"https://ws-public.interpol.int/notices/v1/red?&ageMin={rl}&ageMax={rl}&resultPerPage=160&nationality={k}"
            r =  requests.get(b_agrn_url)
            data = json.loads(r.text)
            if data["total"] > 160:
                print(f"{k}---->{data['total']}")
                for g in ["M","F","U"]:
                    b_u_url = b_agrn_url+"&sexId=" + g
                    rm =  requests.get(b_u_url)
                    mdata = json.loads(rm.text)
                    try:
                        out_list = out_list + datascraper(data)
                    except:
                        print(f"Error:{b_u_url}")
                    
            if data["total"]!=0:
                try:
                    out_list = out_list + datascraper(data)
                except:
                    print(f"Error:{b_agrn_url}")
                datac = datac+data["total"]
                # print(datac)
        print(f'->{datac}')
    else:
        try:
            out_list = out_list + datascraper(data)
        except Exception as e:
            print(f"Error:{b_agr_url}")
            print(f"Error:{e}")
            
with open('inter.json', "w",encoding='utf-8') as outfile:
    json.dump(out_list, outfile,ensure_ascii=False,indent=2)