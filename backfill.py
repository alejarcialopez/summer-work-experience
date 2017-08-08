import csv
from datetime import *
import calendar
import requests
import argparse


def parse_post(dbfile, options):
    location = 1
    url = options.url
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pass}
    count = 1
    postvalues = ""
    with open(dbfile, 'r') as content:
        headers = content.readline()
        line1 = content.readline()
        stop = datetime.strptime(options.stopd, '%Y-%m-%d')
        for line in content:
            list1 = []
            list2 = []
            line2 = content.readline()
            list1.append(line1.split(','))
            list2.append(line2.split(','))
            date1 = datetime.strptime(list1[0][0], "%Y-%m-%d")
            pairdate1 = int(calendar.timegm(date1.timetuple()) *
                            1000000000)
            date2 = datetime.strptime(list2[0][0], "%Y-%m-%d")
            pairdate2 = int(calendar.timegm(date2.timetuple()) *
                            1000000000)
            if list2[0][0] != '' and date2 <= stop:
                if list1[0][2] == 'True\n' and list2[0][2] == 'True\n':
                    if list1[0][0] == list2[0][0]:
                        count += 1
                    elif list1[0][0] != list2[0][0]:

                        for x in range(pairdate1, pairdate2, options.step):
                            postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                                          f'ds_index=0,ds_name=value,' \
                                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                                          f'plugin=statsd,state=ok,type=gauge ' \
                                          f'value={count} {x}\n'
                            # r = requests.post(url, params=payload, data=postvalues)
                line1 = line2
        postvalues = f'{options.timeseries},type_instance=num_provisioned_users,' \
                     f'ds_index=0,ds_name=value,' \
                     f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                     f'plugin=statsd,state=ok,type=gauge ' \
                     f'value={count} {x + options.step}'
        # r = requests.post(url, params=payload, data=postvalues)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("dbfile", type=str, help="file to be parsed")
    parser.add_argument("timeseries", type=str, help="name of time series")
    parser.add_argument("step", type=int, help="interval of how quickly measurements are taken")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, default='4000-12-31', action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    parser.add_argument("url", type=str, help="server url")
    parser.add_argument("influx_db", type=str, help="database to write to")
    parser.add_argument("influx_user", type=str, help="username")
    parser.add_argument("influx_pass", type=str, help="password")
    args = parser.parse_args()
    parse_post(args.dbfile, args)

else:
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    args = parser.parse_args()
