import random
import time
from pytrends.request import TrendReq
import pandas as pd
import time
import pandas as pd
from pytrends.request import TrendReq

# set gmail credentials and path to extract data

# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

# Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()

kwsets = []
daily_data = {} #for each keyword, array of [date,count] tuples

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
    "los angeles": {
        "STATE": "CA",
        "DMA": 803,
    },
    "miami": {
        "STATE": "FL",
        "DMA": 528,
    },
    "new york": {
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
}'

ozone_keywords = [
    ['ozone', 'smog', 'air pollution', 'haze', 'soot'],
    ['ozone', 'code orange', 'code red', 'O3', 'smoke'],
    ['ozone', 'asthma', 'respiratory illness', 'respiratory infection', 'lung disease'],
    ['ozone', 'Chronic obstructive pulmonary disease', 'COPD', 'bronchitis', 'asthma attack'],
    ['ozone', 'headache', 'cough', 'coughing', 'wheezing'],
    ['ozone', 'chest pain', 'chest tightness', 'shortness of breath', 'throat irritation'],
    ['ozone', 'difficulty breathing', 'lung irritation', 'inhaler', 'rapid breathing'],
    ['ozone', 'heart murmur', 'bronchiolitis', 'snoring', 'irregular heartbeat'],
]

nitrogen_keywords = [
    ['nitrogen dioxide', 'no2', 'n02', 'air pollution', 'air pollutant'],
    ['nitrogen dioxide', 'nitrogen oxides', 'oxides of nitrogen', 'smoke', ],
    ['nitrogen dioxide', 'traffic emission', 'tailpipe', 'traffic exposure'],
    ['nitrogen dioxide', 'smog', 'smoggy', 'code orange', 'code red'],
    ['nitrogen dioxide', 'brown', 'brownish', 'reddish-brown', 'haze'],
    ['nitrogen dioxide', 'asthma', 'respiratory illness', 'respiratory infection', 'lung disease'],
    ['nitrogen dioxide', 'Chronic obstructive pulmonary disease', 'COPD', 'bronchitis', 'asthma attack'],
    ['nitrogen dioxide', 'headache', 'coughing', 'wheezing', 'rapid breathing'],
    ['nitrogen dioxide', 'chest pain', 'chest tightness', 'shortness of breath', 'throat irritation'],
    ['nitrogen dioxide', 'difficulty breathing', 'lung irritation', 'inhaler', 'irregular heartbeat'],
]

particulate_matter = [
    ['particulate matter', 'pm2.5', 'pm5', 'particulate matter pollution', 'fine particulates'],
    ['particulate matter', 'air pollution', 'fine particles', 'fine inhalable particles', 'particle pollution'],
    ['particulate matter', 'wildfires', 'power plants', 'industrial pollution', 'organic carbon'],
    ['particulate matter', 'traffic', 'traffic emission', 'tailpipe', 'sulfate'],
    ['particulate matter', 'black carbon', 'elemental carbon', 'soot', 'smog'],
    ['particulate matter', 'haze', 'code orange', 'code red', 'air pollution alert'],
    ['particulate matter', 'grey', 'blue', 'pink', 'asthma'],
    ['particulate matter', 'respiratory illness', 'respiratory infection', 'lung disease', 'bronchitis'],
    ['particulate matter', 'chronic obstructive pulmonary disease', 'COPD', 'lung cancer', 'cardiovascular disease'],
    ['particulate matter', 'premature death',  'Rapid heart rate', 'rapid pulse', 'rapid breathing'],
    ['particulate matter', 'asthma attack', 'coughing', 'arrhythmia', 'wheezing'],
    ['particulate matter', 'chest tightness', 'shortness of breath', 'throat irritation', 'inhaler use'],
    ['particulate matter', 'childhood asthma', 'pediatric asthma'],
]

start_dates = [
    '2007-01-01', '2007-07-01', '2008-01-01', '2008-07-01',
    '2009-01-01', '2009-07-01', '2010-01-01', '2010-07-01',
    '2011-01-01', '2011-07-01', '2012-01-01', '2012-07-01',
    '2013-01-01', '2013-07-01', '2014-01-01', '2014-07-01',
    '2015-01-01', '2015-07-01', '2016-01-01', '2016-07-01',
    '2017-01-01', '2015-07-01', '2018-01-01', '2018-07-01',
    # '2019-01-01',
]

end_dates = [
    '2007-06-30', '2008-01-31', '2008-06-30', '2009-01-31',
    '2009-06-30', '2010-01-31', '2010-06-30', '2011-01-31',
    '2011-06-30', '2012-01-31', '2012-06-30', '2013-01-31',
    '2013-06-30', '2014-01-31', '2014-06-30', '2015-01-31',
    '2015-06-30', '2016-01-31', '2016-06-30', '2017-01-31',
    '2017-06-30', '2018-01-31', '2018-06-30', '2019-01-31',
    # '2019-06-30',
]

all_data_by_kw = []
for kw in kwsets:
    data_by_chunk = []
    for i in range(0, len(start_dates)):
        start = start_dates[i]
        end = end_dates[i]
        tm = start + ' ' + end
        time.sleep(random.randrange(1,5))
        pytrend.build_payload(kw_list=kw,
                      geo=GEO,
                      timeframe=tm
                      )

        # Interest Over Time
        tdf = pytrend.interest_over_time()
        print(tdf.head())
        data_by_chunk.append(tdf)

    kw_data = pd.concat(data_by_chunk)
    print(kw_data.head())
    all_data_by_kw.append(kw_data)

all_data = pd.concat(all_data_by_kw, axis=1)
print(all_data.head())
csv_data = all_data.to_csv("keywords_data.csv")



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



