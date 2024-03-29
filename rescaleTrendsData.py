from datetime import datetime

import pandas as pd
import sys

# read into array for each keyword
# drop "is complete"
# foreach keyword
# work backwords, scale factor = 1. for overlap regions, compute scale between means

if len(sys.argv) == 2:
    input_filename = sys.argv[1]
else:
    print("Please pass in a file name.")
    exit()

data = pd.read_csv(input_filename)

# drops isParital columns
cols = [c for c in data.columns if c[:9] != 'isPartial']

# Create a df of the columns in the variable cols
data = data[cols]

cols = list(data)
cols.remove('date')
rescaled = pd.DataFrame(columns=cols)

for kw in cols:
    if kw == 'date':
        continue

    first_run = True
    date_prev = None
    scale = 1.0
    seen_dates = {}

    for idx in reversed(data.index):
        row = data.loc[idx]
        dt = row['date']
        count = row[kw]  # scale with respect to ozone

        date_new = datetime.strptime(dt, "%Y-%m-%d")

        if (not (date_prev is None)) and (date_new > date_prev):  # update scale factor
            if first_run:
                past_avg = data[kw].iloc[idx - 31: idx].replace(0, pd.np.NaN).mean()
                first_run = False
            else:
                past_avg = rescaled[kw].iloc[idx - 31: idx].replace(0, pd.np.NaN).mean()

            future_avg = data[kw].iloc[idx + 1: idx + 32].replace(0, pd.np.NaN).mean()
            scale = future_avg / max(past_avg, 1)

        date_prev = date_new

        if dt in seen_dates:  # skip overlapped dates
            continue

        scaled = count * scale
        # add scaled value to output dataframe
        rescaled.loc[dt, kw] = scaled
        seen_dates[dt] = 1

    print("Finished stitching keywords through time: ", kw)

rescaled.to_csv(input_filename.replace('.csv', '_stitched.csv'))
