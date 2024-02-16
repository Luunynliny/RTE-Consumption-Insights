from get_data import get_data_rte
import pandas as pd


def get_weekly_forecast(token, start_date, end_date):
    url = "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts"
    return get_data_rte(url, token, start_date, end_date)


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


def get_weekly_forecast_data(token, start_date, end_date):
    response = get_weekly_forecast(token, start_date, end_date)
    df = table_weekly_forecast(response)
    df_clean = clean_data_forecast(df)

    return df_clean
