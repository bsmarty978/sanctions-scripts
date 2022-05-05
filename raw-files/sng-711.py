import camelot
import hashlib
from datetime import datetime

tb = camelot.read_pdf("raw-files/file-157415407029476.pdf",pages="1",flavor="stream")
main = tb[0].df
new_header = main.iloc[0] #grab the first row for the header
main = main[1:] #take the data less the header row
main.columns = new_header #set the header row as the df header


last_updated_string = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def get_hash(n):
    return hashlib.sha256(((n+"Central Bank of Iraq Public Black List, Iraq" + "IRQ_T30064").lower()).encode()).hexdigest()

out_list = []
for i,row in main.astype(str).iterrows():
    pass_dict = {}
    name = row["English Name\n يزيلجنلااب مسلاا"]
    code = row["Code\nزمرلا"]
    pass_dict["uid"] = get_hash(code+name)
    pass_dict["name"] = name

    pass_dict["alias_name"] = [row["Arabic Name\n يبرعلاب مسلاا"]]

    pass_dict["list_type"] = "Entity"
    pass_dict["nns_status"] = "False"
    pass_dict["last_updated"] = last_updated_string
    pass_dict["list_id"] = "IRQ_T30064"
    pass_dict["country"] = ["Iraq"]

    comp = row["English Address\nيزيلجنلااب ناونعلا"]
    comp = comp.split(" / ")
    comp.reverse()
    comp = ",".join(comp)
    pass_dict["address"] = []
    pass_dict["address"].append({
        "complete_address": comp,
        "country" : "Iraq"
    })
    pass_dict["sanction_details"] = {}
    pass_dict["entity_details"] = {}
    pass_dict["comment"] = {}
    pass_dict["sanction_list"] = {}
    # pass_dict["sanction_list"] = {
    #         "sl_authority": "Department of Budget and Treasury, Monaco",
    #         "sl_url": "https://en.service-public-entreprises.gouv.mc/Conducting-business/Legal-and-accounting-obligations/Asset-freezing-measures/National-asset-freezing-and-economic-resources-list",
    #         "watch_list": "European Watchlists",
    #         "sl_host_country": "Monaco",
    #         "sl_type": "Sanctions",
    #         "sl_source": "Monaco National Asset Freeze Economic Resources Sanctions List, Monaco",
    #         "sl_description": "List of individuals and entities sanctioned by Department of Budget and Treasury, Monaco"
    #     }

    out_list.append(pass_dict)
    