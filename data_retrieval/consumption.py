from functions import get_api_response, time_iterator
import pandas as pd
import os


def consumption_response_to_df(response):
    data_json = response.json()
    return pd.DataFrame(data_json['consolidated_power_consumption'][0]['values'])


def clean_consumption_data(df):
    
    date_columns = ['start_date', 'end_date', 'updated_date']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, utc=True)
    
    column_mapping = {
        'value': 'consumption_value',
        'start_date': 'consumption_start_date',
        'end_date': 'consumption_end_date',
        'updated_date': 'consumption_updated_date',
        'status': 'consumption_status'
    }
    df = df.rename(columns=column_mapping)
    
    consumption_primary_key = ['consumption_start_date']
    df_clean = df.drop_duplicates(consumption_primary_key)
    
    return df_clean
    

def get_consumption_data(start_date, end_date):
    
    # get the data from the API and convert it to a table, but we can only get 155 days at a time
    dfs = []
    for start, end in time_iterator(start_date, end_date):        
        response_consumption = get_api_response(str(start).replace(" ", "T"), str(end).replace(" ", "T"), "consumption")        
        df = consumption_response_to_df(response_consumption)
        dfs.append(df)
    df_concat = pd.concat(dfs, ignore_index=True)

    # clean the data
    df_consumption = clean_consumption_data(df_concat)
    
    return df_consumption


if __name__ == "__main__":
    
    start_date = "2019-01-01T00:00:00+01:00"
    end_date = "2020-01-01T00:00:00+01:00"
    
    df_consumption = get_consumption_data(start_date, end_date)

    # Save the dataframe to a csv file for testing purposes
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data/consumption_test.csv')
    df_consumption.to_csv(file_path, mode='w', index=False)
