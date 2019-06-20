import sys
import random
import time
from pytrends.request import TrendReq
import pandas as pd


# set gmail credentials and path to extract data

# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

# Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()

# GEO Codes for our areas
DMA_CODES = {
    "atlanta": {
        "STATE": "GA",
        "DMA": 524,
    },
    "boston": {
        "STATE": "MA",
        "DMA": 506,
    },
    "chicago": {
      "STATE": "IL",
      "DMA": 602,
    },
    "dallas": {
        "STATE": "TX",
        "DMA": 623,
    },
    "houston": {
        "STATE": "TX",
        "DMA": 618,
    },
    "los_angeles": {
        "STATE": "CA",
        "DMA": 803,
    },
    "miami": {
        "STATE": "FL",
        "DMA": 528,
    },
    "new_york": {
        "STATE": "NY",
        "DMA": 501,
    },
    "philadelphia": {
        "STATE": "PA",
        "DMA": 504,
    },
    "washington": {  # DC
        "STATE": "DC",
        "DMA": 511,
    },
}

default_keywords = [
    ['ozone', 'blue', 'clothes', 'love', 'shoes'],  # a set of keywords that are pretty consistent over the last decade
    ['ozone', 'air pollutant', 'air pollution alert', 'air pollution', 'arrhythmia'],
    ['ozone', 'asthma attack', 'asthma', 'black carbon', 'blue'],
    ['ozone', 'bronchiolitis', 'bronchitis', 'brown', 'brownish'],
    ['ozone', 'cardiovascular disease', 'chest pain', 'chest tightness', 'childhood asthma'],
    ['ozone', 'chronic obstructive pulmonary disease', 'code orange', 'code red', 'COPD'],
    ['ozone', 'cough', 'coughing', 'difficulty breathing', 'elemental carbon'],
    ['ozone', 'fine inhalable particles', 'fine particles', 'fine particulates', 'grey'],
    ['ozone', 'haze', 'headache', 'heart murmur', 'industrial pollution'],
    ['ozone', 'inhaler use', 'inhaler', 'irregular heartbeat', 'lung cancer'],
    ['ozone', 'lung disease', 'lung irritation', 'n02', 'nitrogen dioxide'],
    ['ozone', 'nitrogen oxides', 'no2', 'O3', 'organic carbon'],
    ['ozone', 'oxides of nitrogen', 'traffic emission', 'particle pollution', 'particulate matter pollution', ],
    ['ozone', 'particulate matter', 'pediatric asthma', 'pink', 'pm2.5'],
    ['ozone', 'pm5', 'power plants', 'premature death', 'rapid breathing'],
    ['ozone', 'Rapid heart rate', 'rapid pulse', 'reddish-brown', 'respiratory illness'],
    ['ozone', 'respiratory infection', 'shortness of breath', 'smog', 'smoggy'],
    ['ozone', 'smoke', 'snoring', 'soot', 'sulfate'],
    ['ozone', 'tailpipe', 'throat irritation', 'traffic emission', 'traffic exposure'],
    ['ozone', 'traffic', 'wheezing', 'wildfires'],
]

start_dates = [
    '2007-01-01', '2007-07-01', '2008-01-01', '2008-07-01',
    '2009-01-01', '2009-07-01', '2010-01-01', '2010-07-01',
    '2011-01-01', '2011-07-01', '2012-01-01', '2012-07-01',
    '2013-01-01', '2013-07-01', '2014-01-01', '2014-07-01',
    '2015-01-01', '2015-07-01', '2016-01-01', '2016-07-01',
    '2017-01-01', '2017-07-01', '2018-01-01', '2018-07-01',
    # '2019-01-01',
]

end_dates = [
    '2007-07-31', '2008-01-31', '2008-07-31', '2009-01-31',
    '2009-07-31', '2010-01-31', '2010-07-31', '2011-01-31',
    '2011-07-31', '2012-01-31', '2012-07-31', '2013-01-31',
    '2013-07-31', '2014-01-31', '2014-07-31', '2015-01-31',
    '2015-07-31', '2016-01-31', '2016-07-31', '2017-01-31',
    '2017-07-31', '2018-01-31', '2018-07-31', '2019-01-31',
    # '2019-06-30',
]

US = "US"
intercity_scaling_word = "discover"
keyword_set_size_threshold = 100


def generate_keyword_sets(input_filename):
    keywords_file = open(input_filename, "r")

    full_keyword_set = []
    inner_keyword_set = []
    initial_keyword_list = []
    join_word = None
    count = -1
    for line in keywords_file:
        count = count + 1
        fixed_line = line.strip('\n').lower()
        if count == 0:
            join_word = fixed_line
        initial_keyword_list.append(fixed_line)
        if count == keyword_set_size_threshold:
            keywords_file.close()
            break

    initial_keyword_set = set(initial_keyword_list)
    count = 0

    full_keyword_set.append([join_word, intercity_scaling_word])
    inner_keyword_set = [join_word]
    for word in initial_keyword_set:
        if word == join_word:
            continue
        count = count + 1
        inner_keyword_set.append(word)
        if count % 4 == 0:
            full_keyword_set.append(inner_keyword_set)
            inner_keyword_set = [join_word]
    full_keyword_set.append(inner_keyword_set)

    return full_keyword_set


def submit_dma_based_query(output_filename, dma, kwsets, starting_kwset=0):
    all_data_by_kw = []
    state = dma['STATE']
    dma_code = dma['DMA']
    geo_code = US + "-" + state + "-" + str(dma_code)
    print("GEO Code: " + geo_code)

    for kw in kwsets[starting_kwset:]:
        data_by_chunk = []
        for i in range(0, len(start_dates)):
            start_date = start_dates[i]
            end_date = end_dates[i]
            tm = start_date + ' ' + end_date
            time.sleep(random.randrange(2, 5))
            pytrend.build_payload(kw_list=kw,
                                  geo=geo_code,
                                  timeframe=tm
                                  )

            # Interest Over Time
            tdf = pytrend.interest_over_time()
            print(tdf.head())
            data_by_chunk.append(tdf)

        kw_data = pd.concat(data_by_chunk)
        all_data_by_kw.append(kw_data)
        pd.concat(all_data_by_kw).to_csv(output_filename + "_" + kw[1] + ".csv")

    all_data = pd.concat(all_data_by_kw, axis=1)
    print(all_data.head())
    csv_data = all_data.to_csv(output_filename + ".csv")


# If "all" is passed in we run query all of our cities
if len(sys.argv) == 4:
    keyword_set = generate_keyword_sets(sys.argv[2])
    submit_dma_based_query(sys.argv[1], DMA_CODES[sys.argv[1]], keyword_set, sys.argv[3])
elif len(sys.argv) == 3:
    keyword_set = generate_keyword_sets(sys.argv[2])
    if sys.argv[1] == "all":
        for city, dma in DMA_CODES.items():
            submit_dma_based_query(city, dma, keyword_set,)
    else:
        submit_dma_based_query(sys.argv[1], DMA_CODES[sys.argv[1]], keyword_set)
elif len(sys.argv) == 2:
    if sys.argv[1] == "all":
        for city, dma in DMA_CODES.items():
            submit_dma_based_query(city, dma, default_keywords,)
    else:
        submit_dma_based_query(sys.argv[1], DMA_CODES[sys.argv[1]], default_keywords)
else:
    print("Input Invalid: City_Name, Keywords_File, Keywords starting position if terminated early")
    print("Note: the first word in the keywords file will be the joining word (word used to rescale data)")


# Daily_Data = []


# define daily pull code
# def GT_Daily_Run(keys):
#     path = 'path'
#
#     # connect to Google
#     connector = pyGTrends(google_username, google_password)
#     # make request
#     connector.request_report(keys, date="today 90-d", geo="US")
#     # wait a random amount of time between requests to avoid bot detection
#     time.sleep(randint(5, 10))
#     # download file
#     connector.save_csv(path, '_' + "GT_Daily" + '_' + keys.replace(' ', '_'))
#
#     name = path + '_' + "GT_Daily" + '_' + keys.replace(' ', '_')
#
#     with open(name + '.csv', 'rt') as csvfile:
#         csvReader = csv.reader(csvfile)
#         data = []
#
#         for row in csvReader:
#             if any('2015' in s for s in row):
#                 data.append(row)
#
#         day_df = pd.DataFrame(data)
#         cols = ["Day", keys]
#         day_df.columns = [cols]
#         Daily_Data.append(day_df)
#
#
# keywords = ['soccer', 'football', 'baseball']
#
# map(lambda x: GT_Daily_Run(x), keywords)
#
# rge = [Daily_Data[0], Daily_Data[1], Daily_Data[2]]
#
# df_final_daily = reduce(lambda left, right: pd.merge(left, right, on='Day'), rge)
# df_final_daily = df_final_daily.loc[:, (df_final_daily != "0").any(axis=0)]
# df_final_daily.to_csv("Daily_Trends_Data.csv", index=False)



