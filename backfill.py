from datetime import *
import calendar
import requests
import argparse
import validators
import getpass
import io
import fileinput
chars = ['/', '_', '-']


def parse_post(options):
    freq = 1
    lFreq = []
    lDates = []
    url = options.url
    if options.influx_pass is None:
        options.influx_pass = getpass.getpass("password: ")
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pass}
    count = 1
    if isinstance(args.dbfile, io.TextIOWrapper):
        dbfile = options.dbfile.name
    else:
        dbfile = 0
    with open(dbfile, 'r') as content:
        content.readline()
        stop = options.stopd
        for line in content:
            list1 = []
            list2 = []
            line1 = line
            line2 = content.readline()
            list1.append(line1.split(','))
            list2.append(line2.split(','))
            try:
                date1 = datetime.strptime(list1[0][0], "%Y-%m-%d")
                pairdate1 = int(calendar.timegm(date1.timetuple()) *
                                1000000000)
                date2 = datetime.strptime(list2[0][0], "%Y-%m-%d")
                pairdate2 = int(calendar.timegm(date2.timetuple()) *
                                1000000000)
            except ValueError:
                break
            if list2[0][0] != '' and date2 <= stop:
                if list1[0][2] == 'True\n' and list2[0][2] == 'True\n':
                    print(date1, list1[0][1], "\t", date2, list2[0][1])
                    if list1[0][0] == list2[0][0]:
                        count += 2
                        freq += 2
                    elif date1 != date2:
                        lDates.append(f"{date1.year}-{date1.month}")
                        if pairdate2 < pairdate1:
                            b = pairdate1
                            a = pairdate2
                        else:
                            a = pairdate1
                            b = pairdate2

                        if not options.dryrun:
                            print(a, b)
                        postvalues = ""
                        dryrunstr = f'{options.timeseries} (other post values) value={count} {a}'
                        for x in range(a, b, options.step):
                            postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                                          f'ds_index=0,ds_name=value,' \
                                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                                          f'plugin=statsd,state=ok,type=gauge ' \
                                          f'value={count} {x}\n'
                        if not options.dryrun:
                            pass
                            # requests.post(url, params=payload, data=postvalues)
                        elif options.dryrun:
                            pass
                            # print(dryrunstr)
                    lFreq.append(freq)
                    freq = 1
    print(lDates)
    print(lFreq)

if __name__ == "__main__":
    def ts(timesrs):
        test = str(timesrs)
        if 48 <= ord(test[0]) <= 57:
            raise argparse.ArgumentTypeError(f'invalid character "{test[0]}", no numbers at the start')
        for letter in test:
            if letter in chars:
                pass
            elif 48 <= ord(letter) <= 57:
                pass
            elif ord(letter) < 65:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
            elif 90 < ord(letter) < 97:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
            elif ord(letter) > 122:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
        return test


    def user_database(string):
        test = str(string)
        if 48 <= ord(test[0]) <= 57:
            raise argparse.ArgumentTypeError(f'invalid character "{test[0]}", no numbers at the start')
        for letter in test:
            if letter in chars[1:2]:
                pass
            elif 48 <= ord(letter) <= 57:
                pass
            elif ord(letter) < 65:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
            elif 90 < ord(letter) < 97:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
            elif ord(letter) > 122:
                raise argparse.ArgumentTypeError(f'invalid character "{letter}"')
        return test


    def url(link):
        if not validators.url(link):
            raise argparse.ArgumentTypeError('malformed url')
        return link


    def interval(x):
        unit = ""
        units = ['h', 'm', 's', 'ms', 'us', 'ns', ""]
        step = ""
        for char in x:
            try:
                int(char)
                step += char
            except ValueError:
                unit += char
        if unit == units[0]:
            newstep = 3600000000000 * int(step)
            return newstep
        elif unit == units[1]:
            newstep = 60000000000 * int(step)
            return newstep
        elif unit == units[2]:
            newstep = 1000000000 * int(step)
            return newstep
        elif unit == units[3]:
            newstep = 1000000 * int(step)
            return newstep
        elif unit == units[4]:
            newstep = 1000 * int(step)
            return newstep
        elif unit == units[5] or unit == units[6]:
            return int(step)
        else:
            argparse.ArgumentParser().error(f"only choose this units: {units}")

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--databasefile", dest="dbfile", type=argparse.FileType('r'),
                        default=fileinput.input(), help="file to be parsed")
    parser.add_argument("-d", "--stopdate", dest="stopd", type=lambda d: datetime.strptime(d, '%Y-%m-%d'),
                        default=str(date.max),
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    parser.add_argument("-t", "--timeseries", dest="timeseries", type=ts,
                        required=True, help="name of time series")
    parser.add_argument("-b", "--serverdb", dest="influx_db", type=user_database, required=True,
                        help="database to write to")
    parser.add_argument("-u", "--serveruser", dest="influx_user", type=user_database, required=True, help="username")
    parser.add_argument("-i", "--interval", dest="step", type=interval, required=True,
                        help="interval and unit (default unit: ns. e.g: 2h")
    parser.add_argument("-l", "--link", dest="url", type=url,
                        required=True, help="server url")
    parser.add_argument("-p", "--serverpass", dest="influx_pass", type=str, help="password")
    parser.add_argument("-r", "--dryrun", dest="dryrun", type=bool, default=False, choices=[True, 1])
    args = parser.parse_args()

    parse_post(args)

else:
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    args = parser.parse_args()
