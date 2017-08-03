import csv
from datetime import *
import calendar
import requests
import secrets

# f = open('tsandv.txt', 'w')


def parsing(dbfile, timeseries, step):
    count = 1
    lTimestamps = []
    ldates = []
    stopdate = datetime.strptime("2017-07-17", "%Y-%m-%d")
    with open(dbfile, 'r') as DBtoparse:
        content = csv.DictReader(DBtoparse)
        for line in content:
            if line['active'] == 'True' and datetime.strptime(line['start_date'], "%Y-%m-%d") <= stopdate:
                keydate = datetime.strptime(line['start_date'], "%Y-%m-%d")
                ldates.append(int(calendar.timegm(keydate.timetuple()) * 1000000000))
    print(ldates)
    for location in range(0, len(ldates) - 2):
            if ldates[location] == ldates[location + 1]:
                count += 1
            elif ldates[location] != ldates[location + 1]:
                lTimestamps.append({"ts": ldates[location],
                                    "v": count})
                count = 1

    url = 'http://ss-metrics.srv.uis.private.cam.ac.uk:8086/write'
    payload = {"db": "aa2012", "u": secrets.INFLUX_USER, "p": secrets.INFLUX_PASS}
    rollingtotal = 0
    k = 0
    for k in range(0, len(lTimestamps) - 1):
        postvalues = ""
        rollingtotal += lTimestamps[k]['v']
        for x in range(lTimestamps[k]['ts'], lTimestamps[k + 1]['ts'], step):
            postvalues += f'{timeseries},type_instance=num_provisioned_users,' \
                          f'ds_index=0,ds_name=value,' \
                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                          f'plugin=statsd,state=ok,type=gauge ' \
                          f'value={rollingtotal} {x}\n'
        # r = requests.post(url, params=payload, data=postvalues)
        rollingtotal += lTimestamps[k]['v']

    postvalues = f'{timeseries},type_instance=num_provisioned_users,' \
                 f'ds_index=0,ds_name=value,' \
                 f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                 f'plugin=statsd,state=ok,type=gauge ' \
                 f'value={lTimestamps[k + 1]["v"] + rollingtotal} {x + step}'

    # r = requests.post(url, params=payload, data=postvalues)

    # print(r.status_code)
    # print(r.reason)
    print("good")
# for element in range(0, len(newdictionary) - 1):
#    f.write(str(newdictionary[element]))
#    f.write("\n")

# f.close()

parsing('dropbox_licenses2.csv', 'statsd/gauge-num_provisioned_users', 2000000000)
