import pandas as pd
import scipy as sc

# read into array for each keyword
# drop "is complete"
# foreach keyword
# work backwords, scale factor = 1. for overlap regions, compute scale between means

data = pd.read_csv('keywords_data_rescaled_joined.csv')

target_name = 'O3_M8_SCH1'
MIN = 30
K = 3


def computeCorrelations(idata, min=0, avgdays=1):
    # target = pd.DataFrame(idata)[target_name]#.replace(pd.np.NaN, 0)#
    target_orig = idata[target_name]
    target = target_orig.rolling(window=avgdays).mean()  # .replace(0, pd.np.NaN)
    cols = list(data)
    # mins = [0, 30, 40, 45, 50, 55, 60, 70, 75]
    shifts = [-1, 0, 1, 2, 3]

    target[target < min] = pd.np.NaN
    no3 = target.count()
    print("Num days with O3 above threshold: ", no3)

    # 1st quartile:  < 29.3 ppb
    # 2nd quartile: 29.4 –39.7 ppb
    # 3rd quartile:  39.8 – 53.8 ppb
    # 4th quartile: > 53.9 ppb

    # #quartiles
    # target[target <= 29.3] = 1
    # target[(target > 29.3) & (target <= 39.7)] = 2
    # target[(target > 39.7) & (target <= 53.8)] = 3
    # target[(target > 53.8)] = 4
    # target[(target > 70)]=5

    # #four levels
    # target[target <= 29.3] = 1
    # target[(target > 29.3) & (target <= 53.8)] = 2
    # target[(target > 53.8) & (target <= 70)] = 3
    # target[(target > 70)]=4

    # three levels only
    # target[target <= 29.3] = 1
    # target[(target > 29.3) & (target <= 53.8)] = 2
    # target[(target > 53.8) ] = 3

    # three levels only
    # target[target < 50] = 1
    # target[(target > 50) & (target <= 70)] = 2
    # target[(target > 70) ] = 3

    print("keyword\tN\t", end="")
    for shift in shifts:
        print("Pearson(%d)\tSpearman(%d)\t" % (shift, shift), end="")

    print("")

    for col in cols:
        if col == target_name or col == 'date' or col == 'DATE' or col == 'isCold' or col == 'isWarm': continue
        print(col, "\t", end="")

        var = data[col].replace(0, pd.np.NaN)
        var = var.rolling(window=avgdays).mean()
        N = var.count()
        print(N, "\t", end="")

        for shift in shifts:
            var1 = var.shift(periods=shift)
            corelp = var1.corr(target, method='pearson')
            corelc = var1.corr(target, method='spearman')

            print("%.4f\t%.4f\t" % (corelp, corelc), end="")

        print("")

    print("\n")


# print(target.head())
# repeat for cold and warm months
# April 15–October 14

# all data
# print("All Data (whole year)")
# computeCorrelations(data, MIN, K)
data['isCold'] = False
data['isWarm'] = False


def isColdWarm(row):
    dt = row['date']
    # get month, day
    fields = dt.split("/")
    if len(fields) < 3:
        return row

    month = int(fields[0])
    day = int(fields[1])

    if (month >= 4) and (month <= 10):
        if (month == 4) and (day < 15):
            row['isCold'] = True
            return row
        if (month == 10) and (day > 14):
            row['isCold'] = True
            return row

        row['isWarm'] = True
        return row
    else:
        row['isCold'] = True
        return row


# get cold data
data = data.apply(isColdWarm, axis=1)
# print(data.head())
data_cold = data[data['isCold'] == True]
data_warm = data[data['isWarm'] == True]

for min in [0, 30, 40, 50, 60, 70]:
    print("Minimum O3 level = ", min, "running average days = ", K)

    # print(data_cold.head())
    print("Cold months")
    computeCorrelations(data_cold, min, K)

    # get warm data
    print("Warm months")
    computeCorrelations(data_warm, min, K)
