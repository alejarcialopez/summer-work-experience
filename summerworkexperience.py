import csv
from datetime import *
import calendar
import requests
import secrets
import argparse

f = open('dropbox_licenses2.csv', 'r')

linelist = f.readlines()
lastline = linelist[len(linelist) - 1].split(",")

parser = argparse.ArgumentParser(description="parse a CSV of metrics and backfill an InfluxDB timeseries with values")
parser.add_argument("dbfile", type=str, help="file to be parsed")
parser.add_argument("timeseries", type=str, help="name of time series")
parser.add_argument("step", type=int, help="interval of how quickly measurements are taken")
parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                    default=lastline[0], help="store stop date (default: last date of file), date format: YYYY-MM-DD")
parser.add_argument("url", type=str, help="InfluxDB server url")
parser.add_argument("database", type=str, help="InfluxDB database to write to")
parser.add_argument("user", type=str, help="InfluxDB username")
parser.add_argument("password", type=str, help="InfluxDB password")

args = parser.parse_args()


def parsing(dbfile=args.dbfile):
    count = 1
    lTimestamps = []
    ldates = []
    try:
        stop = datetime.strptime(args.stopd, "%Y-%m-%d")
    except TypeError or ValueError:
        print("follow format YYYY-MM-DD")
        exit()
    with open(dbfile, 'r') as DBtoparse:
        content = csv.DictReader(DBtoparse)
        for line in content:
            if line['active'] == 'True' and datetime.strptime(line['start_date'], "%Y-%m-%d") <= stop:
                keydate = datetime.strptime(line['start_date'], "%Y-%m-%d")
                ldates.append(int(calendar.timegm(keydate.timetuple()) * 1000000000))
    for location in range(0, len(ldates) - 2):
            if ldates[location] == ldates[location + 1]:
                count += 1
            elif ldates[location] != ldates[location + 1]:
                lTimestamps.append({"ts": ldates[location],
                                    "v": count})
                count = 1
    # check1
    print("it worked once")
    return lTimestamps

lValues = parsing()


def postvalues(timeseries=args.timeseries, step=args.step, serverurl=args.url,
               INFLUX_DB=args.database, INFLUX_USER=args.user, INFLUX_PASS=args.password):
    url = serverurl
    payload = {"db": INFLUX_DB, "u": INFLUX_USER, "p": INFLUX_PASS}
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
        # r = requests.post(url, params=payload, data=postvalues)
        rollingtotal += lValues[k]['v']

    postvalues = f'{timeseries},type_instance=num_provisioned_users,' \
                 f'ds_index=0,ds_name=value,' \
                 f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                 f'plugin=statsd,state=ok,type=gauge ' \
                 f'value={lValues[k + 1]["v"] + rollingtotal} {x + step}'
    # check2
    print("it worked twice")
    # r = requests.post(url, params=payload, data=postvalues)

if __name__ == "__main__":
    postvalues()
