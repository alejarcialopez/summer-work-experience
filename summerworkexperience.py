import csv
from datetime import *
import calendar
import requests
import secrets

# f = open('tsandv.txt', 'w')
activeLicences = []
unsortedates = []
count = 1
lTimestamps = []
with open('dropbox_licenses2.csv', 'r') as licensesDB:
    content = csv.DictReader(licensesDB)
    for line in content:
        if line['active'] == 'True':
            keyDate = datetime.strptime(line['start_date'], "%Y-%m-%d")
            activeLicences.append([keyDate, line['active']])
            unsortedates.append(keyDate)

sortedates = sorted(unsortedates)
stopdate = datetime.strptime("2017-07-17", "%Y-%m-%d").date()

for x in range(0, len(sortedates) - 2):
        if sortedates[x].date() <= stopdate:
            if sortedates[x] == sortedates[x + 1]:
                count += 1
            elif sortedates[x] != sortedates[x + 1]:
                lTimestamps.append({"ts": int(calendar.timegm(sortedates[x].timetuple()) * 1000000000),
                                    "v": count})
                count = 1


newdictionary = []
url = 'http://ss-metrics.srv.uis.private.cam.ac.uk:8086/write'
payload = {"db": "aa2012", "u": INFLUX_USER, "p": INFLUX_PASS}
rollingtotal = 0
for k in range(0, len(lTimestamps) - 1):
    postValues = ""
    rollingtotal += lTimestamps[k]['v']
    for x in range(lTimestamps[k]['ts'], lTimestamps[k + 1]['ts'], 2000000000):
        # newdictionary.append({'ts': x, 'v': lTimestamps[k]['v'] + rollingtotal})
        postValues += f'statsd/gauge-num_provisioned_users,type_instance=num_provisioned_users,' \
                      f'ds_index=0,ds_name=value,' \
                      f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                      f'plugin=statsd,state=ok,type=gauge ' \
                      f'value={rollingtotal} {x}\n'
    r = requests.post(url, params=payload, data=postValues)
    rollingtotal += lTimestamps[k]['v']

postValues = f'statsd/gauge-num_provisioned_users,type_instance=num_provisioned_users,' \
             f'ds_index=0,ds_name=value,' \
             f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
             f'plugin=statsd,state=ok,type=gauge ' \
             f'value={lTimestamps[k + 1]["v"] + rollingtotal} {x + 2000000000}'

r = requests.post(url, params=payload, data=postValues)

print(r.status_code)
print(r.reason)
# for element in range(0, len(newdictionary) - 1):
#    f.write(str(newdictionary[element]))
#    f.write("\n")

# f.close()
#print(lTimestamps)
