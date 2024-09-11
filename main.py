#!/usr/bin/env python3
import collections
import struct
import os
import sys
import datetime
from io import BytesIO
import operator
import argparse
try:
    import matplotlib.pyplot as plt
    from matplotlib import axes
    from matplotlib.dates import DateFormatter, DayLocator, HourLocator
except ImportError:
    plt = None

def Bcd(data):
    """
    Byte-coded decimal.
    Similar to binary-coded decimal, but using a whole byte for each decimal
    digit instead of a nibble.
    """
    result = 0
    for char in data:
        result = result * 10 + char
    return result

def int3(data):
    return struct.unpack('>I', b'\x00' + data)[0]

def dailyDuration(data):
    #hour, minute = divmod(struct.unpack('>H', data)[0], 100)
    #return datetime.time(hour, minute)
    return struct.unpack('>H', data)[0]

TIMEDELTA = datetime.timedelta(0, 60)
# First possible time. Used as a None replacement for sorting purpose.
EARLIEST_TIME = datetime.datetime(2000, 1, 1)
FIRST_SENSOR_ID_FILENAME_PREFIX = 'A'.encode('ascii')[0]

class Data(object):
    """
    To use:
    - call accumulate() with file data for each available file from a given
      sensor
    - access properties:
      power (kWh)
      recorded (float: hours) - broken on my el4k
      on (float: hours) - broken on my el4k
      sensor_id (0..9)
      price1 (float)
      price2 (float)
      since (datetime.datetime)
      daily_total (list of 3-tuples)
        power (kWh)
        recorded (datetime.time) - broken on my el4k
        on (datetime.time) - broken on my el4k
      record_list (list of 4-tuples)
        date (datetime.datetime)
        voltage (float)
        current (float)
        cosPhi (float)

    Missing from data file:
    - currency
    - mains frequency
    """
    power = None
    recorded = None
    on = None
    sensor_id = None
    price1 = None
    price2 = None
    since = None

    def __init__(self):
        self.record_list = []
        self.daily_total = []

    def accumulate(self, data):
        if data[:5] == b'INFO:':
            assert self.power is None
            read = BytesIO(data[5:]).read
            self.power = int3(read(3)) / 1000.
            self.recorded = int3(read(3)) / 100.
            self.on = int3(read(3)) / 100.
            daily_total = self.daily_total
            for _ in range(10):
                daily_total.append([int3(read(3)) / 1000.])
            for i in range(10):
                daily_total[i].append(dailyDuration(read(2)))
            for i in range(10):
                daily_total[i].append(dailyDuration(read(2)))
            self.sensor_id = read(1)[0]
            self.price1 = Bcd(read(4)) / 1000.
            self.price2 = Bcd(read(4)) / 1000.
            hour, minute, month, day, year = struct.unpack('BBBBB', read(5))
            self.since = datetime.datetime(2000 + year, month, day, hour,
                minute)
            tail = read()
            assert tail == b'\xff\xff\xff\xff', tail
        else:
            read = BytesIO(data).read
            date = None
            while True:
                chunk = read(3)
                if chunk == b'\xe0\xc5\xea':
                    month, day, year, hour, minute = struct.unpack('BBBBB',
                        read(5))
                    date = datetime.datetime(2000 + year, month, day, hour,
                        minute)
                elif chunk == b'\xff\xff\xff':
                    break
                else:
                    voltage, current, power_factor = struct.unpack('>HHB',
                        chunk + read(2))
                    self.record_list.append([date, voltage / 10., current / 1000.,
                        power_factor / 100.])
                    if date is not None:
                        date += TIMEDELTA
            # Thankfuly, python sort is stable, so it won't destroy the order
            # of None-timestamp entries.
            self.record_list.sort(key=lambda x: x[0] or EARLIEST_TIME)
        if self.since is not None and self.record_list and \
                self.record_list[0][0] is None:
            # First measure session is not (may not be ?) preceeded by a
            # time marker. Fill in from header file's first measure time.
            date = self.since
            for record in self.record_list:
                if record[0] is not None:
                    break
                record[0] = date
                date += TIMEDELTA

def parseTime(value):
    return datetime.time(*(int(x) for x in value.split(':')))

def getPrice1RangeList(price1_start_list, price2_start_list):
    merged_list = sorted(
        [(x, True) for x in price1_start_list] +
        [(x, False) for x in price2_start_list],
        key=operator.itemgetter(0),
    )
    result = []
    append = result.append
    last_price2_start = None
    running_range = None
    for time, start in merged_list:
        if start:
            if running_range is None:
                running_range = time
            else:
                sys.stderr.write('Warning: more than one consecutive '
                    '--price1 time given: %s, already applying price1 '
                    'since %s\n' % (time, running_range))
        else:
            if running_range is None:
                if last_price2_start is not None:
                    sys.stderr.write('Warning: more than one consecutive '
                        '--price2 time given: %s, already applying price2 '
                        'since %s\n' % (time, last_price2_start))
            else:
                append((running_range, time))
                running_range = None
            last_price2_start = time
    if running_range is not None:
        append((running_range, None))
    return result

PROCE_HELP_TEMPLATE = 'Apply price %i starting at given time each day. Can ' \
    'be specified several times. Format: <hh>[:<mm>[:<ss>]]'

def main():
    # TODO: annotation (text indexed by sensor_id & time and having a
    # duration, describing a graph chunk)
    parser = argparse.ArgumentParser(description="Parser for Voltcraft's "
        "Energy Logger 4000 data files")
    parser.add_argument('--price1', help=PROCE_HELP_TEMPLATE % 1,
        action='append', default=[], type=parseTime)
    parser.add_argument('--price2', help=PROCE_HELP_TEMPLATE % 2,
        action='append', default=[], type=parseTime)
    parser.add_argument('--text', action='store_true', help='Display text '
        'dump independently of matplotlib availability')
    parser.add_argument('file_list', metavar='file', nargs='+')
    args = parser.parse_args()
    price1_range_list = getPrice1RangeList(args.price1, args.price2)
    sensor_dict = collections.defaultdict(Data)
    basename = os.path.basename
    for filename in args.file_list:
        sensor_id = ord(basename(filename)[0].upper()) - \
            FIRST_SENSOR_ID_FILENAME_PREFIX
        assert 0 <= sensor_id <= 9, sensor_id
        with open(filename, 'rb') as infile:
            data = infile.read()
        sensor_dict[sensor_id].accumulate(data)
    if args.text or plt is None:
        for sensor_id, data in sensor_dict.items():
            assert sensor_id == data.sensor_id, (sensor_id, data.sensor_id)
            print('sensor', data.sensor_id, 'recorded=%s on=%s price1=%.3f ' \
                'price2=%.3f' % (data.recorded, data.on, data.price1, data.price2))
            for day, (power, recorded, on) in enumerate(data.daily_total):
                print('day-%i: %.3fkWh %s %s' % (day, power, recorded, on))
            print('Total: %.3fkWh' % data.power)
            for date, voltage, current, power_factor in data.record_list:
                voltamperes = voltage * current
                print(' %s  %5.01fV %7.03fA %.02fcosPhi %8.2fW %8.2fVA' % (
                    date, voltage, current, power_factor,
                    voltamperes * power_factor, voltamperes
                ))
    else:
        # XXX: looks bad on low resolutions (proportional margins around
        # non-proportional data...)
        cols = len(sensor_dict)
        all_days = DayLocator()
        all_hours = HourLocator()
        day_formatter = DateFormatter('%Y-%m-%d')
        for sensor_id, data in sensor_dict.items():
            date_list = [x[0] for x in data.record_list]
            price_list = []
            for date in date_list:
                if any((min_time <= date.time() < max_time)
                        for min_time, max_time in price1_range_list):
                    price = data.price1
                else:
                    price = data.price2
                price_list.append(price)
            price_list = [x * y[1] * y[2] * y[3] / 60000
                for x, y in zip(price_list, data.record_list)]
            running_price_list = []
            total_price = 0
            for price in price_list:
                total_price += price
                running_price_list.append(total_price)
            expanded_price1_range_list = []
            append = expanded_price1_range_list.append
            first_day = date_list[0].date()
            for day in range((date_list[-1].date() - first_day).days + 1):
                day = first_day + datetime.timedelta(day)
                for start, stop in price1_range_list:
                    append((
                        datetime.datetime.combine(day, start),
                        datetime.datetime.combine(day, stop),
                    ))
            subplot_list = (
                (1, (x[1] for x in data.record_list), 'r.', 'V'),
                (2, (x[2] for x in data.record_list), 'g.', 'A'),
                (3, (x[3] for x in data.record_list), 'k.', 'cos($\\varphi$)'),
                (4, (x[1] * x[2] * x[3] for x in data.record_list), 'c.', 'W'),
                (5, (x[1] * x[2] for x in data.record_list), 'b.', 'VA'),
                # XXX: numbers too small for graph
                (6, price_list, 'y.', 'currency unit / minute'),
                (7, running_price_list, 'y.', 'currency unit'),
            )
            subplot_per_sensor = len(subplot_list)
            for plot, y, line, unit in subplot_list:
                ax = plt.subplot(subplot_per_sensor, cols,
                    (sensor_id * subplot_per_sensor) + plot)
                ax.xaxis.set_major_locator(all_days)
                ax.xaxis.set_minor_locator(all_hours)
                ax.xaxis.set_major_formatter(day_formatter)
                plt.plot(date_list, list(y), line, markersize=2)
                for start, stop in expanded_price1_range_list:
                    plt.axvspan(start, stop, facecolor='y', alpha=0.5)
                plt.ylabel(unit)
        plt.subplots_adjust(0.04, 0.02, 0.99, 0.97, 0.20, 0.10)
        plt.show()

if __name__ == '__main__':
    main()
