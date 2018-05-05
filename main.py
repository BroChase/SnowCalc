import SnowCalc
import pandas as pd


if __name__ == '__main__':

    # Multiyear agg
    # basefile = 'Snow19902016/'
    # df_precip, df_temp, df_snow = SnowCalc.agg_regions_snow(basefile, 26, '199001', '201512')

    # single year
    basefile = '2017/'
    df_precip, df_temp, df_snow = SnowCalc.agg_regions_snow(basefile, 1, '201701', '201712')

    month = SnowCalc.precip_month(df_precip, df_snow)
    temp_month = SnowCalc.temp_month(df_temp)

    df_snow.drop(df_snow.columns[0:366], axis=1, inplace=True)
    df_snow.drop(df_snow.columns[12:], axis=1, inplace=True)

    df = pd.concat([month, temp_month, df_snow], axis=1)
    snow = df['snow']
    df.drop(labels=['snow'], axis=1, inplace=True)
    df['snow'] = snow

    df.to_csv(basefile+'snowtotals.csv')
