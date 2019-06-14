from datetime import datetime

import pandas as pd
import csv

# read into array for each keyword
# drop "is complete"
# foreach keyword
# work backwords, scale factor = 1. for overlap regions, compute scale between means

OVERLAP = 31.0

data = pd.read_csv('keywords_data.csv')
ozone = pd.read_csv('ozone_level.csv')

# Create a variable that drop columns with column names where the first three letters of the column names was 'pre'
cols = [c for c in data.columns if c[:9] != 'isPartial']

# Create a df of the columns in the variable cols
data = data[cols]

cols = list(data)
rescaled = pd.DataFrame(columns=cols)
rescaled.drop('date', axis=1)

# for kw in cols:
#    ma = '_ma_' + kw
#    data[ma] = data.rolling(31, on=kw)

for kw in cols:
    if kw == 'date':
        continue

    date_prev = None
    back_avg = 0
    forward_avg = 0
    scale = 1.0
    ma = '_ma_' + kw
    seen_dates = {}
    last_ozone = 'ozone'
    if 'ozone' in kw:
        last_ozone = kw

    for idx in reversed(data.index):
        row = data.loc[idx]
        dt = row['date']

        date_new = datetime.strptime(dt, "%Y-%m-%d")

        count = row[kw]  # scale with respect to ozone
        denum = row[last_ozone] + 1
        normalized_count = count / denum

        if (not (date_prev is None)) and (date_new > date_prev):  # update scale factor
            past_avg = data[kw].iloc[idx - 31: idx].replace(0, pd.np.NaN).mean()
            future_avg = data[kw].iloc[idx + 1: idx + 32].replace(0, pd.np.NaN).mean()
            scale = future_avg / max(past_avg, 1)

        date_prev = date_new

        if dt in seen_dates:  # skip overlapped dates
            continue

        scaled = count * scale
        # add scaled value to output dataframe
        rescaled.loc[dt, kw] = scaled
        seen_dates[dt] = 1

    print("Finished rescaling keywords: ", kw)

# save backup data
# joined = pd.merge(rescaled, ozone, left_on='date', right_on='DATE')
# print(joined.head())
# correlations w, shift & lag

rescaled.to_csv('keywords_data_rescaled.csv')
