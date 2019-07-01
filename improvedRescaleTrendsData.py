import sys
from datetime import datetime

import numpy as np
import pandas as pd

# STATIC VARIABLES
# common month between time ranges used for stitching
STITCHING_MONTHS = (1, 7)  # january and july
# common word between keyword sets used for scaling
CROSS_SET_KEYWORD = 'ozone'


def scale_monthly_data(unscaled_monthly_data, cross_set_join_word=CROSS_SET_KEYWORD):
    first_cross_set_keyword_occurrence = True
    first_cross_set_keyword_mean = None
    rescaled_monthly_table = None
    keyword_scale = 1.0

    for keyword in unscaled_monthly_data.columns:
        if keyword == 'date':
            rescaled_column = unscaled_monthly_data['date']
            pass
        else:
            if cross_set_join_word in keyword:
                cross_set_monthly_mean = unscaled_monthly_data[keyword].mean(skipna=True)
                if first_cross_set_keyword_occurrence:
                    first_cross_set_keyword_mean = cross_set_monthly_mean
                    first_cross_set_keyword_occurrence = False
                else:
                    keyword_scale = cross_set_monthly_mean / first_cross_set_keyword_mean

            rescaled_column = unscaled_monthly_data[keyword].apply(lambda x: x * keyword_scale)
        rescaled_monthly_table = pd.concat([rescaled_monthly_table, rescaled_column], sort=True, axis=1)
    return rescaled_monthly_table


def combine_and_scale_keyword_sets(data, months_to_scale, cross_set_join_word=CROSS_SET_KEYWORD):
    # scales keyword sets between a chosen join word
    list_of_rescaled_months = list()
    for last_day_in_month in months_to_scale:
        first_day_in_month = datetime(last_day_in_month.year, last_day_in_month.month, 1).strftime("%Y-%m-%d")
        last_day_in_month = last_day_in_month.strftime("%Y-%m-%d")
        print("Joining keyword sets: " + last_day_in_month)

        duplicate_first_day_in_month_list = np.where(data["date"] == first_day_in_month)[0]
        duplicate_last_day_in_month_list = np.where(data["date"] == last_day_in_month)[0]

        monthly_data = data.iloc[duplicate_first_day_in_month_list[0]: duplicate_last_day_in_month_list[0] + 1]
        scaled_monthly_data = scale_monthly_data(monthly_data, cross_set_join_word)
        list_of_rescaled_months.append(scaled_monthly_data)

        # rescales the duplicate months which are needed later for stitching
        if len(duplicate_first_day_in_month_list) > 1 and len(duplicate_last_day_in_month_list) > 1:
            monthly_data = data.iloc[duplicate_first_day_in_month_list[1]: duplicate_last_day_in_month_list[1] + 1]
            scaled_monthly_data = scale_monthly_data(monthly_data, cross_set_join_word)
            list_of_rescaled_months.append(scaled_monthly_data)

    scaled_keyword_sets = pd.concat(list_of_rescaled_months, sort=True)
    return scaled_keyword_sets


def stitch_keywords(data, months_to_stitch, overlapping_months=STITCHING_MONTHS):
    is_first_run = True
    first_slice_index = None
    list_of_stitched_time_ranges = list()
    scale = pd.DataFrame(index=data.columns)
    scale['scale'] = 1

    for last_day_in_month in months_to_stitch:
        if last_day_in_month.month in overlapping_months:
            first_day_in_month = datetime(last_day_in_month.year, last_day_in_month.month, 1).strftime("%Y-%m-%d")
            last_day_in_month = last_day_in_month.strftime("%Y-%m-%d")
            print("Stitch dates: " + str(last_day_in_month))

            # list of start and end indices current the duplicate months (months used to stitch)
            duplicate_first_day_in_month_list = np.where(data["date"] == first_day_in_month)[0]
            duplicate_last_day_in_month_list = np.where(data["date"] == last_day_in_month)[0]

            if is_first_run and first_slice_index is None:
                first_slice_index = duplicate_first_day_in_month_list[0]
                continue
            else:
                # creates a df with first 6 months of original data
                time_range = data.iloc[first_slice_index: duplicate_last_day_in_month_list[0] + 1]
                time_range = time_range.set_index('date')

                # scales each column
                if not is_first_run:  # scale is 1 for first run
                    for kw in cols:
                        if kw == 'date':
                            pass
                        else:
                            time_range[kw] = time_range[kw].apply(lambda x: x * scale.loc[kw])

                list_of_stitched_time_ranges.append(time_range)

                if len(duplicate_first_day_in_month_list) == 1 or len(duplicate_last_day_in_month_list) == 1:
                    break

                if is_first_run:
                    is_first_run = False
                    past_avg = data.iloc[duplicate_first_day_in_month_list[0]: duplicate_last_day_in_month_list[0] + 1].replace(0, pd.np.NaN).mean(axis=0)
                else:
                    past_avg = time_range.loc[first_day_in_month: last_day_in_month].replace(0, pd.np.NaN).mean(axis=0)

                future_avg = data.iloc[duplicate_first_day_in_month_list[1]: duplicate_last_day_in_month_list[1] + 1].replace(0, pd.np.NaN).mean(axis=0)
                past_avg = past_avg.fillna(1)
                future_avg = future_avg.fillna(1)
                scale = future_avg / past_avg
                first_slice_index = duplicate_last_day_in_month_list[1] + 1

    return pd.concat(list_of_stitched_time_ranges, sort=True)


input_filename = None
if len(sys.argv) > 1:
    if ".csv" not in sys.argv[1]:
        input_filename = sys.argv[1] + ".csv"
    else:
        input_filename = sys.argv[1]
else:
    print("Please pass in a file name and a (optional: ozone is default) common keyword set join word.")
    exit()


trends = pd.read_csv(input_filename)

# drops columns named partial
cols = [c for c in trends.columns if c[:9] != 'isPartial']
trends = trends[cols]

# assumes the list of dates is the first column
first_date_in_table = trends.iloc[0, 0]
last_date_in_table = trends.iloc[-1, 0]
months_in_table = pd.date_range(start=first_date_in_table, end=last_date_in_table, freq='M')

if len(sys.argv) == 3:
    scaled_trends = combine_and_scale_keyword_sets(trends, months_in_table, sys.argv[2])
else:
    scaled_trends = combine_and_scale_keyword_sets(trends, months_in_table)

scaled_trends = scaled_trends[cols]
scaled_trends.to_csv(input_filename.replace('.csv', '_scaled.csv'))

stitched_and_scaled_trends = stitch_keywords(scaled_trends, months_in_table, STITCHING_MONTHS)
stitched_and_scaled_trends.to_csv(input_filename.replace('.csv', '_stitched.csv'))
