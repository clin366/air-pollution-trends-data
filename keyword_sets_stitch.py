from datetime import datetime
import pandas as pd
import numpy as np
import sys

input_filename = None
if len(sys.argv) == 2:
    input_filename = sys.argv[1]
else:
    print("Please pass in a file name.")
    exit()

JANUARY = 1
JULY = 7

trends = pd.read_csv(input_filename)

# drops columns named partial
cols = [c for c in trends.columns if c[:9] != 'isPartial']
trends = trends[cols]
cols.remove('date')

is_first_run = True
is_first_slice_index_set = False
first_slice_index = None
last_slice_index = None

# creates a list of all the months in our data's time range
first_date_in_table = trends.iloc[0, 0]
last_date_in_table = trends.iloc[-1, 0]
months_in_table = pd.date_range(start=first_date_in_table, end=last_date_in_table, freq='M')

for kw in cols:

    for last_day_in_month in months_in_table:
        if last_day_in_month.month == JANUARY or last_day_in_month.month == JULY:
            year = last_day_in_month.year
            month = last_day_in_month.month
            if not is_first_slice_index_set:
                is_first_slice_index_set = True
                first_slice_date = datetime(year, month, 1).strftime("%Y-%m-%d")
                duplicate_dates = np.where(trends["date"] == first_slice_date)[0]  # returns touple and not an array
                print(duplicate_dates)
                print(type(duplicate_dates))
                if len(duplicate_dates) == 1:  # should only equal one on our very first run
                    continue
                else:
                    past_avg = trends[kw].iloc[duplicate_dates[0]: duplicate_dates[1]].replace(0, pd.np.NaN).mean()


#
#
#
#
#
#
#         is_first_slice_index = True
#         first_slice_index =
#     else:
#         is_first_slice_index = False
#         last_slice_index = last_day_in_month
#
#
#
#
#     if first_run:
#         first_run = False
#         continue
#     else:
#         overlapping_dates = np.where(trends["date"] == first_day_string)
#         if len(overlapping_dates) > 1:
#
#
#
#
#
# first_day_string = first_day_in_month.strftime("%Y-%m-%d")
# last_day_string = last_day_in_month.strftime("%Y-%m-%d")
#
#
#
#
#
# print(last_day_in_month.month)
print("Done")
