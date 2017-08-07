import csv
from datetime import *
import calendar
import requests
import argparse


def parse_post(dbfile, options):
    location = 1
    url = options.url
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pass}
    rollingtotal = 0
    count = 1
    lTimestamps = []
    ldates = []
    pairdate1 = None
    pairdate2 = None
    postvalues = ""
    with open(dbfile, 'r') as DBtoparse:
        content = list(csv.DictReader(DBtoparse))
        if args.stopd:
            try:
                stopdate = datetime.strptime(args.stopd, "%Y-%m-%d")
            except TypeError or ValueError:
                print("follow date format: YYYY-MM-DD")
                exit()
            for line in content:
                try:
                    nextline = content[location]
                except IndexError:
                    nextline = content[location - 1]
                if line['active'] == 'True':
                    if datetime.strptime(line['start_date'], "%Y-%m-%d") <= stopdate and \
                                    line['start_date'] == nextline['start_date']:

                        pairdate1 = int(calendar.timegm(datetime.strptime(line['start_date'], "%Y-%m-%d")).timetuple() *
                                        1000000000)
                        count += 1
                    elif datetime.strptime(line['start_date'], "%Y-%m-%d") <= stopdate and \
                            line['start_date'] != nextline['start_date']:

                        pairdate2 = int(
                            calendar.timegm(datetime.strptime(nextline['start_date'], "%Y-%m-%d")).timetuple() *
                            1000000000)
                        rollingtotal += count
                        if pairdate1 is None:
                            date1 = datetime.strptime(line['start_date'], "%Y-%m-%d")
                            pairdate1 = int(calendar.timegm(date1.timetuple()) *
                                            1000000000)
                        for x in range(pairdate1, pairdate2, options.step):
                            postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                                          f'ds_index=0,ds_name=value,' \
                                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                                          f'plugin=statsd,state=ok,type=gauge ' \
                                          f'value={rollingtotal} {x}\n'
                            # r = requests.post(url, params=payload, data=postvalues)
                        count = 1
                        location += 1
        else:
            for line in content:
                try:
                    nextline = content[location]
                except IndexError:
                    nextline = content[location - 1]
                if line['active'] == 'True' and line['start_date'] == nextline['start_date']:
                    date1 = datetime.strptime(line['start_date'], "%Y-%m-%d")
                    pairdate1 = int(calendar.timegm(date1.timetuple()) *
                                    1000000000)
                    count += 1
                elif line['active'] == 'True' and line['start_date'] != nextline['start_date']:
                    date2 = datetime.strptime(nextline['start_date'], "%Y-%m-%d")
                    pairdate2 = int(calendar.timegm(date2.timetuple()) *
                                    1000000000)
                    rollingtotal += count
                    if pairdate1 is None:
                        date1 = datetime.strptime(line['start_date'], "%Y-%m-%d")
                        pairdate1 = int(calendar.timegm(date1.timetuple()) *
                                        1000000000)
                    for x in range(pairdate1, pairdate2, options.step):
                        postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                                      f'ds_index=0,ds_name=value,' \
                                      f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                                      f'plugin=statsd,state=ok,type=gauge ' \
                                      f'value={rollingtotal} {x}\n'
                        # r = requests.post(url, params=payload, data=postvalues)
                    count = 1
                location += 1

    # check1
    print("it worked!!!!!!!!!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("dbfile", type=str, help="file to be parsed")
    parser.add_argument("timeseries", type=str, help="name of time series")
    parser.add_argument("step", type=int, help="interval of how quickly measurements are taken")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
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
