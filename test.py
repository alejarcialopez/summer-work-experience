from datetime import *
import calendar
import requests
import argparse
import validators
import getpass
import io
import fileinput
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import json

chars = ['/', '_', '-']
f = open('activeusers.json', 'w')


def parse_post(options):
    lunixts = list()
    freq = 1
    lFreq = []
    lusers = list()
    lDates = []
    url = options.url
    if options.influx_pass is None:
        options.influx_pass = getpass.getpass("password: ")
    payload = {"db": options.influx_db, "u": options.influx_user, "p": options.influx_pass}
    count = 1
    if isinstance(args.dbfile, io.TextIOWrapper):
        dbfile = options.dbfile.name
    else:
        dbfile = ""
    with open(dbfile, 'r') as content:
        content.readline()
        stop = options.stopd
        line1 = content.readline()
        for line in content:
            list1 = []
            list2 = []
            line2 = line
            list1.append(line1.split(','))
            list2.append(line2.split(','))
            if list1[0][2] == 'True\n' and list2[0][2] == 'True\n':
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
                    if list1[0][0] == list2[0][0]:
                        count += 1
                        freq += 1
                    elif date1 != date2:
                        if date1.month != date2.month:
                            lDates.append(f"{date1.year}-{date1.month}")
                            lusers.append(count)
                        elif date2 == stop:
                            lDates.append(f"{date2.year}-{date2.month}")
                            lusers.append(count)

                        if not options.dryrun:
                            print(pairdate1, pairdate2)
                        postvalues = ""
                        dryrunstr = f'{options.timeseries} (other post values) value={count} {pairdate1}'
                        for x in range(pairdate1, pairdate2, options.step):
                            postvalues += f'{options.timeseries},type_instance=num_provisioned_users,' \
                                          f'ds_index=0,ds_name=value,' \
                                          f'ds_type=gauge,host=selfservice-node4.srv.uis.private.cam.ac.uk,' \
                                          f'plugin=statsd,state=ok,type=gauge ' \
                                          f'value={count} {x}\n'
                        if not options.dryrun:
                            requests.post(url, params=payload, data=postvalues)
                        elif options.dryrun:
                            print(dryrunstr)
                        if date1.month != date2.month:
                            lFreq.append(freq)
                            freq = 1
                        elif date2 == stop:
                            lFreq.append(freq)
            line1 = line2
    xticks = list()
    for axis in range(0, len(lDates)):
        xticks.append(axis)
    plotdata = [xticks, lDates, lFreq]
    for n in lDates:
        element = datetime.strptime(n, '%Y-%m')
        lunixts.append(int(calendar.timegm(element.timetuple()) *
                       1000))
    zipped = zip(lunixts, lusers)
    f.write(json.dumps(dict(zipped)))
    f.close()
    return plotdata

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

    graph = parse_post(args)

    fig = plt.figure(figsize=[10, 6])
    ax = plt.subplot(111)
    l = ax.fill_between(graph[0], graph[2])

    l.set_facecolors([[.5, .5, .8, .3]])
    l.set_edgecolors([[0, 0, .5, .3]])
    l.set_linewidths([3])
    loc = ticker.MultipleLocator(base=25.0)

    ax.set_ylim(0, max(graph[2]) + 50)
    ax.yaxis.set_major_locator(loc)
    ax.yaxis.set_tick_params(size=0)
    ax.yaxis.set_tick_params(size=0)
    ax.spines['right'].set_color((.8, .8, .8))
    ax.spines['top'].set_color((.8, .8, .8))

    xlab = plt.xlabel('dates (Year-Month)')
    ylab = plt.ylabel('frequency')
    ttl = plt.title('frequency of active licenses per month')

    xlab.set_style('italic')
    xlab.set_size(13)
    ylab.set_style('italic')
    ylab.set_size(13)
    ttl.set_weight('bold')

    plt.grid(True)
    plt.xticks(graph[0], graph[1])
    plt.plot(graph[0], graph[2], color="#868485")
    # plt.show()

else:
    parser = argparse.ArgumentParser(description="parse database of licenses and post rolling total and timestamps")
    parser.add_argument("-sd", "--stopdate", dest="stopd", type=str, action="store",
                        help="store stop date (default: last date of file), date format: YYYY-MM-DD")
    args = parser.parse_args()
