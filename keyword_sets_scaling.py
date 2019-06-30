import sys
from datetime import datetime

import pandas as pd

input_filename = None

# Starting keyword set rescaling
CROSS_SET_KEYWORD = 'ozone'

if len(sys.argv) == 3:
    input_filename = sys.argv[1]
    KEYWORD_SETS_JOINING_WORD = sys.argv[2]
elif len(sys.argv) == 2:
    input_filename = sys.argv[1]
else:
    print("Please pass in a file name.")
    exit()

stitched = pd.read_csv(input_filename, index_col=0)
cols = stitched.columns

latest_date_in_table = stitched.index.values[0]
earliest_date_in_table = stitched.index.values[-1]
months_in_table = pd.date_range(start=earliest_date_in_table, end=latest_date_in_table, freq='M')

list_of_rescaled_months = list()
for last_day_in_month in months_in_table:
    rescaled_monthly_table = None
    year = last_day_in_month.year
    month = last_day_in_month.month
    first_day_in_month = datetime(year, month, 1)

    first_index = first_day_in_month.strftime("%Y-%m-%d")
    last_index = last_day_in_month.strftime("%Y-%m-%d")

    monthly_data = stitched.loc[last_index: first_index, :]

    first_cross_set_keyword_occurrence = True
    first_cross_set_keyword_mean = None

    scale = 1.0

    for kw in cols:
        if CROSS_SET_KEYWORD in kw:
            cross_set_monthly_mean = monthly_data[kw].mean(skipna=True)
            if first_cross_set_keyword_occurrence:
                first_cross_set_keyword_mean = cross_set_monthly_mean
                first_cross_set_keyword_occurrence = False
            else:
                scale = cross_set_monthly_mean / first_cross_set_keyword_mean

        rescaled_column = monthly_data[kw].apply(lambda x: x * scale)
        rescaled_monthly_table = pd.concat([rescaled_monthly_table, rescaled_column], sort=True, axis=1)

    list_of_rescaled_months.append(rescaled_monthly_table)

aggregated_monthly_tables = pd.concat(list_of_rescaled_months, sort=True)
aggregated_monthly_tables.to_csv(input_filename.replace('.csv', '_cross_set_scaled.csv'))
