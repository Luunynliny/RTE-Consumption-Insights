from functions import get_api_response, time_iterator
import pandas as pd
import os


def forecast_response_to_df(response):
    data = response.json()
    df_weekly_forecasts = pd.DataFrame()
    for forecast in data['weekly_forecasts']:
        df = pd.DataFrame(forecast['values'])
        df['updated_date'] = forecast['updated_date']
        df_weekly_forecasts = pd.concat([df_weekly_forecasts, df])
    return df_weekly_forecasts


def clean_forecast_data(df):
    
    date_columns = ['start_date', 'end_date', 'updated_date']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, utc=True)
    
    column_mapping = {
        'value': 'forecast_value',
        'start_date': 'forecast_start_date',
        'end_date': 'forecast_end_date',
        'updated_date': 'forecast_updated_date',
    }
    df = df.rename(columns=column_mapping)
    
    forecast_primary_key = ['forecast_start_date','forecast_updated_date']
    df_clean = df.drop_duplicates(forecast_primary_key)
    
    return df_clean
    

def get_forecast_data(start_date, end_date):
    
    # get the data from the API and convert it to a table, but we can only get 155 days at a time
    dfs = []
    for start, end in time_iterator(start_date, end_date, chunk_size=100):        
        response_forecast = get_api_response(str(start).replace(" ", "T"), str(end).replace(" ", "T"), "forecast")        
        df = forecast_response_to_df(response_forecast)
        dfs.append(df)
    df_concat = pd.concat(dfs, ignore_index=True)

    # clean the data
    df_forecast = clean_forecast_data(df_concat)
    
    return df_forecast


if __name__ == "__main__":
    
    start_date = "2019-01-01T00:00:00+01:00"
    end_date = "2020-01-01T00:00:00+01:00"
    
    df_forecast = get_forecast_data(start_date, end_date)

    # Save the dataframe to a csv file for testing purposes
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data/forecast_test.csv')
    df_forecast.to_csv(file_path, mode='w', index=False)