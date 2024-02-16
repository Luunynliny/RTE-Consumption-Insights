from get_data import get_data_rte
import pandas as pd
import os

def get_consolidated_weekly_forecast(start_date, end_date):
    url = "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts"
    return get_data_rte(url, start_date, end_date)

def table_weekly_forecast(response):
    data = response.json()
    df_weekly_forecasts = pd.DataFrame()
    for forecast in data['weekly_forecasts']:
        df = pd.DataFrame(forecast['values'])
        df['updated_date'] = forecast['updated_date']
        df_weekly_forecasts = pd.concat([df_weekly_forecasts, df])
    return df_weekly_forecasts


def clean_data_forecast(df):
    # Convert 'updated_date' column to datetime format
    df['updated_date'] = pd.to_datetime(df['updated_date'])
    # Rename column 'value' to 'consolidated_power_consumption'
    df = df.rename(columns={'value': 'forecast_value'})
    # Convert start_date column to datetime format
    df['start_date'] = pd.to_datetime(df['start_date'])
    # Convert start_date column to datetime format
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df


if __name__ == "__main__":
    
    start_date = "2021-01-01T00:00:00+01:00"
    end_date = "2021-02-01T00:00:00+01:00"

    response = get_consolidated_weekly_forecast(start_date, end_date)
    df = table_weekly_forecast(response)
    df_clean = clean_data_forecast(df)

    # Get the current directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the file path
    file_path = os.path.join(current_directory, 'data/weekly_forecast.csv')

    # Save df to the file and overwrite if it already exists
    df_clean.to_csv(file_path, mode='w', index=False)
