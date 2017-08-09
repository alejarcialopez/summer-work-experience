from datetime import *
import calendar
import requests
import argparse
import validators

chars = ['/', '_', '-']


def parse_post(options):
    url = options.url
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pass}
    count = 1
    with open(options.dbfile.name, 'r') as content:
        headers = content.readline()
        line1 = content.readline()
        stop = options.stopd
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
                        if pairdate2 < pairdate1:
                            b = pairdate1
                            a = pairdate2
                        else:
                            a = pairdate1
                            b = pairdate2
                        print(a, b)
                        postvalues = ""
                        for x in range(a, b, options.step):
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


    def url(link):
        if not validators.url(link):
            raise argparse.ArgumentTypeError('malformed url')


    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--databasefile", dest="dbfile", type=argparse.FileType('r'), help="file to be parsed")
    parser.add_argument("-d", "--stopdate", dest="stopd", type=lambda d: datetime.strptime(d, '%Y-%m-%d'),
                        default=str(date.max),
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    parser.add_argument("-t", "--timeseries", dest="timeseries", type=ts,
                        required=True, help="name of time series")
    parser.add_argument("-b", "--serverdb", dest="influx_db", type=user_database, required=True,
                        help="database to write to")
    parser.add_argument("-u", "--serveruser", dest="influx_user", type=user_database, required=True, help="username")
    parser.add_argument("-i", "--interval", dest="step", type=int, required=True,
                        help="interval of how quickly measurements are taken")
    parser.add_argument("-l", "--link", dest="url", type=url,
                        required=True, help="server url")
    parser.add_argument("-p", "--serverpass", dest="influx_pass", type=str, required=True, help="password")
    args = parser.parse_args()
    parse_post(args)

else:
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    args = parser.parse_args()
