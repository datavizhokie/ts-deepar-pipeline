# flexible aggregation script

import pandas as pd
import matplotlib.pyplot as plt

def read_data(file, ts_datetime):

    df = pd.read_csv(file)

    # convert to datetime format
    df[ts_datetime] =  pd.to_datetime(df[ts_datetime])


    print(df.dtypes)

    return df

df = read_data('crime.csv', 'REPORTED_DATE')

def create_diff_group_fields(df, ts_datetime):

    # create date field    
    df['date'] = df[ts_datetime].dt.date
    # create year_week, month, and year fields
    df['year_week'] = df[ts_datetime].dt.strftime('%Y-%V')
    df['year'] = df[ts_datetime].dt.strftime('%Y')
    df['month'] = df[ts_datetime].dt.strftime('%m')
    print("Preview of Fields Created:")
    print(df[['date','year_week','year','month']].head(5))

    return df

df = create_diff_group_fields(df, 'REPORTED_DATE')

def aggregate_data(df, granularity, series_field, rec_id):

    df_grouped=df.groupby([granularity,'OFFENSE_CATEGORY_ID'], as_index=False)['OFFENSE_ID'].count()\
    .rename(columns={rec_id:'target'})
    print("Aggregated Data:")
    print(df_grouped.head(5))


    return df_grouped

df_grouped = aggregate_data(df, 'year_week','OFFENSE_CATEGORY_ID', 'OFFENSE_ID')


def create_ref_data(df, granularity, series_field):

    # Get unique Series names and Unique dates
    series_names = df.drop_duplicates([series_field]).reset_index(drop=True)
    series_names = series_names[[series_field]]

    cadence_values = df.drop_duplicates([granularity]).reset_index(drop=True)
    cadence_values = cadence_values[[granularity]]

    print("Total Cadence Values:")
    print(len(cadence_values))

    # Create blown up reference table with all dates and all categories via outer join
    cadence_values['joincol'] = 1
    series_names['joincol'] = 1

    blown_up = pd.merge(left=cadence_values,right=series_names, on='joincol', how='outer')
    del blown_up['joincol']

    # Join reference table to actual data
    df_ref = blown_up.merge(df[[granularity, series_field,'target']], on=[granularity, series_field], how='left').reset_index(drop=True)
    df_ref['target'] = df_ref['target'].fillna(0)

    print("Blown up Reference Table:")
    print(df_ref.head(5))

    return df_ref

df_ref = create_ref_data(df_grouped, 'year_week', 'OFFENSE_CATEGORY_ID')


def viz_something():
    pass

def write_results(df):
    df.to_csv('grouped_data.csv', index=False)

write_results(df_ref)