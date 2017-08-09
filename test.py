import argparse


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

parser.add_argument("-i", "--interval", dest="step", type=interval, required=True,
                    help="interval and unit (default unit: ns")

args = parser.parse_args()

print(args.step)
