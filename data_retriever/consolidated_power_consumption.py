import pandas as pd

from get_data import get_data_rte


def get_consolidated_power_consumption(token, start_date, end_date):
    url = "https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1/consolidated_power_consumption"
    return get_data_rte(url, token, start_date, end_date)


def table_power_consumption(response):
    data_json = response.json()
    return pd.DataFrame(data_json['consolidated_power_consumption'][0]['values'])


def clean_data_power_consumption(df):
    # Convert 'updated_date' column to datetime format
    df['updated_date'] = pd.to_datetime(df['updated_date'])
    # Rename column 'value' to 'consolidated_power_consumption'
    df = df.rename(columns={'value': 'consolidated_power_consumption_value'})
    # Convert start_date column to datetime format
    df['start_date'] = pd.to_datetime(df['start_date'])
    # Convert start_date column to datetime format
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df


def get_consolidated_power_consumption_data(token, start_date, end_date):
    response = get_consolidated_power_consumption(token, start_date, end_date)
    df = table_power_consumption(response)
    df_clean = clean_data_power_consumption(df)

    return df_clean
