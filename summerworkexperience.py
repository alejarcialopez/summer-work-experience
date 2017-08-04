import csv
from datetime import *
import calendar
import requests
import argparse


def getlastline(dbfile):
    f = open(dbfile, 'r')
    linelist = f.readlines()
    lastline = linelist[len(linelist) - 1].split(",")
    f.close()
    return lastline


def parsing(dbfile, stopd):
    lastline = getlastline(dbfile)
    count = 1
    lTimestamps = []
    ldates = []
    try:
        stop = datetime.strptime(stopd, "%Y-%m-%d")
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


def postvalues(timeseries, step, serverurl,
               influx_db, influx_user, influx_pas, lvalues):
    url = serverurl
    payload = {"db": influx_db, "u": influx_user, "p": influx_pas}
    rollingtotal = 0
    k = 0
    for k in range(0, len(lvalues) - 1):
        postvalues = ""
        rollingtotal += lvalues[k]['v']
        for x in range(lvalues[k]['ts'], lvalues[k + 1]['ts'], step):
            postvalues += f'{timeseries},type_instance=num_provisioned_users,' \
                          f'ds_index=0,ds_name=value,' \
                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                          f'plugin=statsd,state=ok,type=gauge ' \
                          f'value={rollingtotal} {x}\n'
        # r = requests.post(url, params=payload, data=postvalues)
        rollingtotal += lvalues[k]['v']

    postvalues = f'{timeseries},type_instance=num_provisioned_users,' \
                 f'ds_index=0,ds_name=value,' \
                 f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                 f'plugin=statsd,state=ok,type=gauge ' \
                 f'value={lvalues[k + 1]["v"] + rollingtotal} {x + step}'
    # check2
    print("it worked twice")
    # r = requests.post(url, params=payload, data=postvalues)

