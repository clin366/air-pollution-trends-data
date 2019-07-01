import sys
from datetime import datetime

import numpy as np
import pandas as pd

input_filename = None
if len(sys.argv) == 2:
    input_filename = sys.argv[1]
else:
    print("Please pass in a file name for stitching.")
    exit()

STITCHING_MONTHS = [1, 7]
CROSS_SET_KEYWORD = 'ozone'

trends = pd.read_csv(input_filename)

# drops columns named partial
cols = [c for c in trends.columns if c[:9] != 'isPartial']
trends = trends[cols]


def scale_monthly_data(unscaled_monthly_data):
    first_cross_set_keyword_occurrence = True
    first_cross_set_keyword_mean = None
    rescaled_monthly_table = None
    keyword_scale = 1.0

    for keyword in cols:
        if keyword == 'date':
            rescaled_column = unscaled_monthly_data['date']
            pass
        else:
            if CROSS_SET_KEYWORD in keyword:
                cross_set_monthly_mean = unscaled_monthly_data[keyword].mean(skipna=True)
                if first_cross_set_keyword_occurrence:
                    first_cross_set_keyword_mean = cross_set_monthly_mean
                    first_cross_set_keyword_occurrence = False
                else:
                    keyword_scale = cross_set_monthly_mean / first_cross_set_keyword_mean

            rescaled_column = unscaled_monthly_data[keyword].apply(lambda x: x * keyword_scale)
        rescaled_monthly_table = pd.concat([rescaled_monthly_table, rescaled_column], sort=True, axis=1)
    return rescaled_monthly_table


def combine_and_scale_keyword_sets(months_to_scale):
    # scales keyword sets between a chosen join word
    list_of_rescaled_months = list()
    for last_datetime_in_month in months_to_scale:
        first_day_in_month = datetime(last_datetime_in_month.year, last_datetime_in_month.month, 1).strftime("%Y-%m-%d")
        last_datetime_in_month = last_datetime_in_month.strftime("%Y-%m-%d")

        duplicate_first_day_in_month_list = np.where(trends["date"] == first_day_in_month)[0]
        duplicate_last_day_in_month_list = np.where(trends["date"] == last_datetime_in_month)[0]

        monthly_data = trends.iloc[duplicate_first_day_in_month_list[0]: duplicate_last_day_in_month_list[0] + 1]
        scaled_monthly_data = scale_monthly_data(monthly_data)
        list_of_rescaled_months.append(scaled_monthly_data)
        if len(duplicate_first_day_in_month_list) > 1 and len(duplicate_last_day_in_month_list) > 1:
            monthly_data = trends.iloc[duplicate_first_day_in_month_list[1]: duplicate_last_day_in_month_list[1] + 1]
            scaled_monthly_data = scale_monthly_data(monthly_data)
            list_of_rescaled_months.append(scaled_monthly_data)

    scaled_keyword_sets = pd.concat(list_of_rescaled_months, sort=True)
    return scaled_keyword_sets


# assumes the list of dates is the first column
first_date_in_table = trends.iloc[0, 0]
last_date_in_table = trends.iloc[-1, 0]
months_in_table = pd.date_range(start=first_date_in_table, end=last_date_in_table, freq='M')

scaled_trends = combine_and_scale_keyword_sets(months_in_table)
scaled_trends = scaled_trends[cols]  # reorder columns to original input
exit()

is_first_run = True
first_slice_index = None
list_of_stitched_time_ranges = list()
scale = pd.DataFrame(index=cols)
scale['scale'] = 1


for last_datetime_in_month in months_in_table:
    if last_datetime_in_month.month in STITCHING_MONTHS:
        first_day_in_month = datetime(last_datetime_in_month.year, last_datetime_in_month.month, 1).strftime("%Y-%m-%d")
        last_datetime_in_month = last_datetime_in_month.strftime("%Y-%m-%d")
        print("\n" + str(last_datetime_in_month))

        # list of indices duplicate start and end dates for current month
        duplicate_first_day_in_month_list = np.where(trends["date"] == first_day_in_month)[0]
        duplicate_last_day_in_month_list = np.where(trends["date"] == last_datetime_in_month)[0]

        if is_first_run and first_slice_index is None:
            first_slice_index = duplicate_first_day_in_month_list[0]
            continue
        else:
            # creates a df with first 6 months of original data
            time_range = trends.iloc[first_slice_index: duplicate_last_day_in_month_list[0] + 1]
            time_range = time_range.set_index('date')

            print(scale)
            # scales each column
            if not is_first_run:  # scale is 1 for first run
                for kw in cols:
                    time_range[kw] = time_range[kw].apply(lambda x: x * scale.loc[kw])

            list_of_stitched_time_ranges.append(time_range)

            if len(duplicate_first_day_in_month_list) == 1 or len(duplicate_last_day_in_month_list) == 1:
                break

            if is_first_run:
                is_first_run = False
                past_avg = trends.iloc[duplicate_first_day_in_month_list[0]: duplicate_last_day_in_month_list[0]+1].replace(0, pd.np.NaN).mean(axis=0)
            else:
                past_avg = time_range.loc[first_day_in_month: last_datetime_in_month].replace(0, pd.np.NaN).mean(axis=0)

            future_avg = trends.iloc[duplicate_first_day_in_month_list[1]: duplicate_last_day_in_month_list[1] + 1].replace(0, pd.np.NaN).mean(axis=0)
            past_avg = past_avg.fillna(1)
            future_avg = future_avg.fillna(1)
            scale = future_avg / past_avg
            first_slice_index = duplicate_last_day_in_month_list[1] + 1

stitched_time_ranges = pd.concat(list_of_stitched_time_ranges, sort=True)
stitched_time_ranges.to_csv(input_filename.replace('.csv', '_stitched.csv'))
print("Done")
