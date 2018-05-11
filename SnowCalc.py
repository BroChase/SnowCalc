import pandas as pd


# basefile: filepath base
# range: how many years of snow totals are being calculated 1 to n
# startdate: range start ex: 199001
# enddate: range end ex: 201501
def agg_regions_snow(basefile, range, startdate, enddate):

    # region list
    # fips_codes = ['08037', '08049', '08051', '08053', '08057', '08065',
    #               '08067', '08069', '08079', '08091', '08097', '08107',
    #               '08109', '08111', '08113', '08117', '08013', '08015',
    #               '08019', '30001', '30007', '30031', '30043', '30061',
    #               '30081', '30087', '53007', '53009', '53037', '53047',
    #               '53053', '53073', '53077']

    fips_codes = ['24023', '42001', '26139', '30031', '36009', '30063', '35027', '51125', '55141', '49005', '41043',
                  '08045', '26047', '16003', '06043', '41063', '34003', '26053', '08113', '55031', '53009', '27157',
                  '53037', '36105', '56039', '50001', '36113', '16049', '08015', '55127', '53073', '08019', '23003',
                  '36069', '26061', '55095', '36023', '16013', '18029', '56001', '36031', '42011', '26029', '50015',
                  '35007', '55061', '30013', '26159', '26083', '55133', '42055', '55131', '30001', '04019', '17031',
                  '53033', '49049', '16005', '49035', '49043', '23001', '25011', '27145', '27163', '26135', '23021',
                  '50021', '06057', '26125', '33019', '08049', '23025', '50025', '01049', '33003', '50023', '35055',
                  '37189', '08007', '55117', '06093', '06071', '19155', '54075', '04001', '36111', '08107', '55097',
                  '33009', '26145', '39085', '55063', '23005', '39139', '56035', '55067', '16079', '36025', '41037',
                  '26103', '38019', '08013', '08067', '23007', '53047', '53063', '06037', '26071', '26005', '27049',
                  '09003', '41059', '30049', '33007', '16031', '39091', '42027', '48041', '16035', '32031', '42111',
                  '36039', '55025', '55059', '08097', '42025', '42049', '32003', '25009', '37087', '42003', '29189',
                  '30009', '16025', '06061', '53065', '36019', '36065', '27041', '29165', '08111', '36043', '33013',
                  '38059', '38009', '36067', '30081', '53041', '16017', '56023', '55081', '27061', '36045', '27007',
                  '36013', '42089', '08117', '35035', '55111', '08077', '08065', '37115', '50005', '30039', '06017',
                  '25021', '09005', '23017', '26131', '04005', '27031', '30057', '35028', '06019', '51017', '51171',
                  '19015', '26089', '30053', '25003', '56025', '33001', '36035', '17043', '19061', '16051', '17085',
                  '41027', '41005', '26165', '44009', '50007', '42103', '08037', '23019', '33011', '36033', '55021',
                  '08051', '53007', '41035', '06109', '09001', '26055', '26137', '42069', '53013', '27013', '25017',
                  '36029', '27005', '18117', '23013', '50027', '33005', '50019', '42009', '30029', '39153', '19153',
                  '26081', '25027', '55109', '42115', '06051', '49021', '25013', '41001', '26009', '46099', '27053',
                  '06003', '06029', '55073', '27137', '49057', '26093', '17161']

    fips_codes.sort()

    df_list_precip = []
    df_list_temp = []
    df_list = []

    for file in fips_codes:
        path = basefile + 'precip/' + file + 'precip.csv'
        path2 = basefile + 'temp/' + file + 'temp.csv'
        # store precipitation file in df
        df = pd.read_csv(path)
        # store temp file in df
        df2 = pd.read_csv(path2)
        df.drop(df.columns[0], axis=1, inplace=True)
        df2.drop(df2.columns[0], axis=1, inplace=True)
        # fill any nan values with 0
        df.fillna(value=0, inplace=True)
        df2.fillna(value=0, inplace=True)
        # append the dateframes to lists of dataframes
        df_list_precip.append(df)
        df_list_temp.append(df2)

        # if the temp is below or equal to 30 true if not false
        df3 = df2[:365].copy()
        df3[df3 <= 30] = 1
        df3[df3 > 30] = 0
        # multiply the precipitation and temp frame to get days it snowed
        result = df.mul(df3, axis=0)
        SOI = soi_month(range, startdate, enddate)
        result = pd.concat([result, SOI], axis=1)
        result['FIPS'] = file
        # sup the precipitations across the row and create a snow column
        result['snow'] = result.iloc[:, :-13].sum(axis=1)
        # precipitation to snow calculation
        result['snow'] = result['snow'].apply(lambda x: x*10)
        # append the results dataframe to list
        df_list.append(result)

    df_precip = pd.concat(df_list_precip, ignore_index=True)
    df_temp = pd.concat(df_list_temp, ignore_index=True)
    df_snow = pd.concat(df_list, ignore_index=True)
    return df_precip, df_temp, df_snow


# Aggregate the days into their respective months
# df_precip: dataframe of precipitations by day
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

    df_precip.drop(df_precip.columns[0:-14], axis=1, inplace=True)

    return df_precip

# Aggregate daily temps into month averages
# df_temp: dateframe with daily temperatures
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

    df_temp.drop(df_temp.columns[:-12], axis=1, inplace=True)

    return df_temp

# get the Southern Oscillation Index
# ep = how many years
# startdate: year to start ex: 199001
# enddate: year to end on ex: 201512
def soi_month(ep,startdate, enddate):

    index = []
    for i in range(ep):
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
    # data.csv contains NOAA historical SOI
    SOI = pd.read_csv('data.csv')
    # get SOI for the given date ranges
    SOI = SOI.loc[startdate:enddate]
    SOI['Southern Oscillation Index (SOI)'] = SOI['Southern Oscillation Index (SOI)'].str.replace(' ','').astype(float)
    soi_month = SOI['Southern Oscillation Index (SOI)']

    df_month = pd.DataFrame({'month': index, 'value': soi_month})
    df_month = df_month.set_index('month')

    m = 12
    n = 0
    df_list = []
    for i in range(ep):
        temp = df_month[n:m].transpose()
        m += 12
        n += 12
        df_list.append(temp)
    df = pd.concat(df_list, ignore_index=True)

    return df
