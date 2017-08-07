import csv
from datetime import *
import calendar
import requests
import argparse


def parsing(dbfile):
    count = 1
    lTimestamps = []
    ldates = []
    with open(dbfile, 'r') as DBtoparse:
        content = csv.DictReader(DBtoparse)
        if args.stopd:
            try:
                stopdate = datetime.strptime(args.stopd, "%Y-%m-%d")
            except TypeError or ValueError:
                print("follow date format: YYYY-MM-DD")
                exit()
            for line in content:
                if line['active'] == 'True' and datetime.strptime(line['start_date'], "%Y-%m-%d") <= stopdate:
                    keydate = datetime.strptime(line['start_date'], "%Y-%m-%d")
                    ldates.append(int(calendar.timegm(keydate.timetuple()) * 1000000000))
        else:
            for line in content:
                if line['active'] == 'True':
                    keydate = datetime.strptime(line['start_date'], "%Y-%m-%d")
                    ldates.append(int(calendar.timegm(keydate.timetuple()) * 1000000000))
    for location in range(0, len(ldates) - 2):
        if ldates[location] == ldates[location + 1]:
            count += 1
        elif ldates[location] != ldates[location + 1]:
            lTimestamps.append({"ts": ldates[location],
                                "v": count})
            count = 1
    return lTimestamps


def postvalues(options, lvalues):
    url = options.serverurl
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pas}
    rollingtotal = 0
    k = 0
    for k in range(0, len(lvalues) - 1):
        postvalues = ""
        rollingtotal += lvalues[k]['v']
        for x in range(lvalues[k]['ts'], lvalues[k + 1]['ts'], options.step):
            postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                          f'ds_index=0,ds_name=value,' \
                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                          f'plugin=statsd,state=ok,type=gauge ' \
                          f'value={options.rollingtotal} {x}\n'
            pass
        r = requests.post(url, params=payload, data=postvalues)
        rollingtotal += lvalues[k]['v']

    postvalues = f'{options.timeseries},type_instance=num_provisioned_users,' \
                 f'ds_index=0,ds_name=value,' \
                 f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                 f'plugin=statsd,state=ok,type=gauge ' \
                 f'value={lvalues[k + 1]["v"] + rollingtotal} {x + options.step}'
    # r = requests.post(url, params=payload, data=postvalues)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("dbfile", type=str, help="file to be parsed")
    parser.add_argument("timeseries", type=str, help="name of time series")
    parser.add_argument("step", type=int, help="interval of how quickly measurements are taken")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    parser.add_argument("url", type=str, help="server url")
    parser.add_argument("database", type=str, help="database to write to")
    parser.add_argument("user", type=str, help="username")
    parser.add_argument("password", type=str, help="password")
    args = parser.parse_args()
    lvalues = parsing(args.dbfile)
    postvalues(args, lvalues)

else:
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    args = parser.parse_args()
