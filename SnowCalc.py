import pandas as pd


def agg_regions_snow():

    fips_codes = ['08037', '08049', '08051', '08053', '08057', '08065',
                  '08067', '08069', '08079', '08091', '08097', '08107',
                  '08109', '08111', '08113', '08117']

    df_list_precip = []
    df_list_temp = []
    df_list = []
    for file in fips_codes:
        path = 'Colorado19902016/precip/'+file+'precip.csv'
        path2 = 'Colorado19902016/temp/'+file+'temp.csv'

        df = pd.read_csv(path)
        df2 = pd.read_csv(path2)
        df.drop(df.columns[0], axis=1, inplace=True)
        df2.drop(df2.columns[0], axis=1, inplace=True)

        df.fillna(value=0, inplace=True)
        df2.fillna(value=0, inplace=True)

        df_list_precip.append(df)
        df_list_temp.append(df2)

        df3 = df2[:365].copy()
        df3[df3 <= 30] = 1
        df3[df3 > 30] = 0

        result = df.mul(df3, axis=0)
        SOI = soi_month()
        result = pd.concat([result, SOI], axis=1)
        result['FIPS'] = file
        result['snow'] = result.iloc[:, :366].sum(axis=1)
        result['snow'] = result['snow'].apply(lambda x: x*10)
        df_list.append(result)

    df_precip = pd.concat(df_list_precip, ignore_index=True)
    df_temp = pd.concat(df_list_temp, ignore_index=True)
    df_snow = pd.concat(df_list, ignore_index=True)
    return df_precip, df_temp, df_snow

def precip_month(df_precip, df_snow):


    df_precip['FIPS'] = df_snow['FIPS']
    df_precip['Jan_p'] = df_precip.iloc[:, 0:30].sum(axis=1)
    df_precip['Feb_p'] = df_precip.iloc[:, 31:58].sum(axis=1)
    df_precip['Mar_p'] = df_precip.iloc[:, 59:89].sum(axis=1)
    df_precip['Apr_p'] = df_precip.iloc[:, 90:119].sum(axis=1)
    df_precip['May_p'] = df_precip.iloc[:, 120:150].sum(axis=1)
    df_precip['Jun_p'] = df_precip.iloc[:, 151:180].sum(axis=1)
    df_precip['Jul_p'] = df_precip.iloc[:, 181:211].sum(axis=1)
    df_precip['Aug_p'] = df_precip.iloc[:, 212:242].sum(axis=1)
    df_precip['Sep_p'] = df_precip.iloc[:, 243:272].sum(axis=1)
    df_precip['Oct_p'] = df_precip.iloc[:, 273:303].sum(axis=1)
    df_precip['Nov_p'] = df_precip.iloc[:, 304:333].sum(axis=1)
    df_precip['Dec_p'] = df_precip.iloc[:, 334:364].sum(axis=1)
    # Leap Year
    df_precip['Feb_p'] += df_precip.iloc[:, 365:365].sum(axis=1)
    df_precip['snow'] = df_snow['snow']

    df_precip.drop(df_precip.columns[0:366], axis=1, inplace=True)

    return df_precip


def temp_month(df_temp):

    df_temp['Jan_t'] = df_temp.iloc[:, 0:30].sum(axis=1) / 31
    df_temp['Feb_t'] = df_temp.iloc[:, 31:58].sum(axis=1)
    df_temp['Mar_t'] = df_temp.iloc[:, 59:89].sum(axis=1) / 31
    df_temp['Apr_t'] = df_temp.iloc[:, 90:119].sum(axis=1) / 30
    df_temp['May_t'] = df_temp.iloc[:, 120:150].sum(axis=1) / 31
    df_temp['Jun_t'] = df_temp.iloc[:, 151:180].sum(axis=1) / 30
    df_temp['Jul_t'] = df_temp.iloc[:, 181:211].sum(axis=1) / 31
    df_temp['Aug_t'] = df_temp.iloc[:, 212:242].sum(axis=1) / 31
    df_temp['Sep_t'] = df_temp.iloc[:, 243:272].sum(axis=1) / 30
    df_temp['Oct_t'] = df_temp.iloc[:, 273:303].sum(axis=1) / 31
    df_temp['Nov_t'] = df_temp.iloc[:, 304:333].sum(axis=1) / 30
    df_temp['Dec_t'] = df_temp.iloc[:, 334:364].sum(axis=1) / 31
    # Leap Year
    df_temp['Feb_t'] += df_temp.iloc[:, 365:365].sum(axis=1)
    df_temp['Feb_t'] = df_temp['Feb_t'] / 28.25

    df_temp.drop(df_temp.columns[0:366], axis=1, inplace=True)

    return df_temp

def soi_month():

    index = []
    for i in range(26):
        index.append('Jan_soi')
        index.append('Feb_soi')
        index.append('Mar_soi')
        index.append('Apr_soi')
        index.append('May_soi')
        index.append('Jun_soi')
        index.append('Jul_soi')
        index.append('Aug_soi')
        index.append('Sep_soi')
        index.append('Oct_soi')
        index.append('Nov_soi')
        index.append('Dec_soi')

    SOI = pd.read_csv('data.csv')
    SOI = SOI.loc['199001':'201512']

    SOI['Southern Oscillation Index (SOI)'] = SOI['Southern Oscillation Index (SOI)'].str.replace(' ','').astype(float)
    soi_month = SOI['Southern Oscillation Index (SOI)']

    df_month = pd.DataFrame({'month': index, 'value': soi_month})
    df_month = df_month.set_index('month')

    m = 12
    n = 0
    df_list = []
    for i in range(26):
        temp = df_month[n:m].transpose()
        m += 12
        n += 12
        df_list.append(temp)
    df = pd.concat(df_list, ignore_index=True)

    return df
