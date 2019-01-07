#!/usr/bin/python3

import datetime

start_date = datetime.datetime(2019, 1, 3, 15, 0, 38)


def f(secs):
    return int(635 * secs ** 0.5)


for seconds in range(0, 9 * 24 * 3600, 3600):
    print(datetime.datetime.fromtimestamp(start_date.timestamp() + seconds), f(seconds), sep=';')
