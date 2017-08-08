import argparse
from datetime import *
import validators
chars = ['/', '_', '-']


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
parser.add_argument("-f", "--databasefile", dest="dbfile", type=open, help="file to be parsed")
parser.add_argument("-d", "--stopdate", dest="stopd", type=lambda d: datetime.strptime(d, '%Y-%m-%d'),
                    default=str(date.max), help="store stop date (default: last date of file), date format: YYYY-MM-DD")
parser.add_argument("-t", "--timeseries", dest="timeseries", type=ts,
                    required=True, help="name of time series")
parser.add_argument("-b", "--serverdb", dest="influx_db", type=user_database, required=True,
                    help="database to write to")
parser.add_argument("-u", "--serveruser", dest="influx_user", type=user_database, required=True, help="username")
parser.add_argument("-i", "--interval", dest="step", type=int, required=True,
                    help="interval of how quickly measurements are taken")
parser.add_argument("-l", "--link", dest="url", type=url,
                    required=True, help="server url")

try:
    args = parser.parse_args()
except FileNotFoundError:
    raise FileNotFoundError("file not found")
