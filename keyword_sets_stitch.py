import sys
from datetime import datetime

import numpy as np
import pandas as pd

input_filename = None
if len(sys.argv) == 2:
    input_filename = sys.argv[1]
else:
    print("Please pass in a file name.")
    exit()

STITCHING_MONTHS = [1, 7]

trends = pd.read_csv(input_filename)

# drops columns named partial
cols = [c for c in trends.columns if c[:9] != 'isPartial']
trends = trends[cols]
cols.remove('date')

df = trends.iloc[0:10]
mean_df = df.mean(axis=0)  # takes mean of each column, idk why axis=0 does columns instead of axis=1
print(mean_df)
print(mean_df.loc['discover'])

is_first_run = True
first_slice_index = None
last_slice_index = None
list_of_stitched_time_ranges = list()

# creates a list of all the months in our data's time range
# assumes the list of dates is the first column
first_date_in_table = trends.iloc[0, 0]
last_date_in_table = trends.iloc[-1, 0]
months_in_table = pd.date_range(start=first_date_in_table, end=last_date_in_table, freq='M')

for last_datetime_in_month in months_in_table:
    if last_datetime_in_month.month in STITCHING_MONTHS:
        first_day_in_month = datetime(last_datetime_in_month.year, last_datetime_in_month.month, 1).strftime("%Y-%m-%d")
        last_day_in_month = last_datetime_in_month.strftime("%Y-%m-%d")

        # list of indices duplicate start and end dates for current month
        duplicate_first_day_in_month_list = np.where(trends["date"] == first_day_in_month)[0]
        duplicate_last_day_in_month_list = np.where(trends["date"] == last_day_in_month)[0]

        #
        if is_first_run and first_slice_index is None:
            first_slice_index = duplicate_first_day_in_month_list[0]
            continue
        else:
            if is_first_run:
                # creates a df with first 6 months of original data
                last_slice_index = duplicate_last_day_in_month_list[0]
                first_time_range = trends.iloc[first_slice_index: last_slice_index + 1]
                list_of_stitched_time_ranges.append(first_time_range.set_index('date'))

                past_avg_month_table_start = duplicate_first_day_in_month_list[0]
                past_avg_month_table_end = duplicate_last_day_in_month_list[0]

                future_avg_month_table_start = duplicate_first_day_in_month_list[1]
                future_avg_month_table_end = duplicate_last_day_in_month_list[1]

                stitched_time_range_table = None
                scale = 1.0
                for kw in cols: # TODO - see if if this can be deleted
                    past_avg = trends[kw].iloc[past_avg_month_table_start: past_avg_month_table_end].replace(0, pd.np.NaN).mean()
                    future_avg = trends[kw].iloc[future_avg_month_table_start: future_avg_month_table_end].replace(0, pd.np.NaN).mean()
                    scale = future_avg / max(past_avg, 1)


    #
    #
    #
    #                 # TODO - pull information from the previously stitched table so that it is continuous
    #
    #             is_first_run = False
    #         else:
    #             print()
    # print()

# past_avg = trends[kw].iloc[duplicate_dates[0]: duplicate_dates[1]].replace(0, pd.np.NaN).mean()
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
