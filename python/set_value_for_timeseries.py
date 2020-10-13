import argparse
import sys
import random
from datetime import datetime, timedelta

parser = argparse.ArgumentParser(description="Script for creating time series data")

#start date, end date tick rate millis, outputfile

parser.add_argument(
'--start_date',
type=datetime.fromisoformat, 
dest='start_date', 
required=True,
help='start date for time series in ISO Format YYYY-MM-DD'
)

parser.add_argument(
'--end_date',
type=datetime.fromisoformat, 
dest='end_date', 
required=True,
help='end date for time series in ISO Format YYYY-MM-DD, last entry in series will be end date - tick rate'
)

parser.add_argument(
'--tick_rate',
type=int, 
dest='tick_rate', 
required=True,
help='rate at which values are produced in milli seconds'
)

parser.add_argument(
'--output_file',
type=argparse.FileType('w'), 
dest='output', 
help='destination file for timeseries, if missing uses STDOUT'
)

parser.add_argument(
'--first_dimension',
type=str, 
dest='first_dimension',
required=True, 
help='first dimension for timeseries'
)

parser.add_argument(
'--start_value',
type=int, 
dest='start_value',
required=True, 
help='start_value for timeseries'
)

parser.add_argument(
'--delimiter',
type=str, 
dest='delimiter',
required=True, 
help='delimiter for output columns'
)


args  = parser.parse_args()

start_date = args.start_date
end_date = args.end_date
tick_rate = args.tick_rate
delimiter = args.delimiter
dimension = args.first_dimension
current_value = args.start_value
output = sys.stdout if args.output is None else args.output
current_tick = start_date

random.seed(42)


def value_generator(value):
    return value + random.randint(-29,30)

while (current_tick < end_date):
    output.write(f"{current_tick:%Y-%m-%d %H:%M:%S.%f}{delimiter}{dimension}{delimiter}{current_value}\n")
    current_tick = current_tick + timedelta(milliseconds=tick_rate)
    current_value = value_generator(current_value)
