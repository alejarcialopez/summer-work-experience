import csv
from datetime import *
import calendar
import requests
import secrets
import argparse

parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
parser.add_argument("dbfile", type=str, help="file to be parsed")
parser.add_argument("timeseries", type=str, help="name of time series")
parser.add_argument("step", type=int, help="interval of how quickly measurements are taken")
args = parser.parse_args()


def parsing(dbfile=args.dbfile):
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
    for location in range(0, len(ldates) - 2):
            if ldates[location] == ldates[location + 1]:
                count += 1
            elif ldates[location] != ldates[location + 1]:
                lTimestamps.append({"ts": ldates[location],
                                    "v": count})
                count = 1
    # check2 print("it worked once")
    return lTimestamps

lValues = parsing()


def postvalues(timeseries=args.timeseries, step=args.step):
    url = 'http://ss-metrics.srv.uis.private.cam.ac.uk:8086/write'
    payload = {"db": "aa2012", "u": secrets.INFLUX_USER, "p": secrets.INFLUX_PASS}
    rollingtotal = 0
    k = 0
    for k in range(0, len(lValues) - 1):
        postvalues = ""
        rollingtotal += lValues[k]['v']
        for x in range(lValues[k]['ts'], lValues[k + 1]['ts'], step):
            postvalues += f'{timeseries},type_instance=num_provisioned_users,' \
                          f'ds_index=0,ds_name=value,' \
                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                          f'plugin=statsd,state=ok,type=gauge ' \
                          f'value={rollingtotal} {x}\n'
        r = requests.post(url, params=payload, data=postvalues)
        rollingtotal += lValues[k]['v']

    postvalues = f'{timeseries},type_instance=num_provisioned_users,' \
                 f'ds_index=0,ds_name=value,' \
                 f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                 f'plugin=statsd,state=ok,type=gauge ' \
                 f'value={lValues[k + 1]["v"] + rollingtotal} {x + step}'
    # check1 print("it worked twice")
    r = requests.post(url, params=payload, data=postvalues)

postvalues()
