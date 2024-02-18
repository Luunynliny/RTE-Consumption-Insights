import pandas as pd
import requests

from .utils import get_rte_api_response, dates_period_iterator, format_date

ROUTE: str = "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts"
DAYS_LIMIT: int = 100


def forecast_response_to_df(response: requests.Response) -> pd.DataFrame:
    """
    Convert RTE API response to pandas dataframe.

    :param response: a response from the RTE API.
    :type response: requests.Response
    :return: the resposne data formatted as a dataframe.
    :rtype: pd.DataFrame
    """
    data = response.json()
    df_weekly_forecasts = pd.DataFrame()

    for forecast in data['weekly_forecasts']:
        df = pd.DataFrame(forecast['values'])
        df['updated_date'] = forecast['updated_date']
        df_weekly_forecasts = pd.concat([df_weekly_forecasts, df])

    return df_weekly_forecasts


def clean_forecast_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the response dataframe.

    :param df: a pandas dataframe.
    :type df: pd.Dataframe
    :return: the cleaned pandas dataframe.
    :rtype: pd.DataFrame
    """
    date_columns = ['start_date', 'end_date', 'updated_date']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, utc=True)

    column_mapping = {
        'value': 'forecast_value',
        'start_date': 'forecast_start_date',
        'end_date': 'forecast_end_date',
        'updated_date': 'forecast_updated_date',
    }
    df = df.rename(columns=column_mapping)

    forecast_primary_key = ['forecast_start_date', 'forecast_updated_date']
    df_clean = df.drop_duplicates(forecast_primary_key)

    return df_clean


def get_forecast_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get the forecast consumption data from the RTE.

    :param start_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type start_date: str
    :param end_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type end_date: str
    :return: a pandas dataframe of the RTE consumption data between two dates.
    :rtype: pd.DataFrame
    """
    # get the data from the API and convert it to a table, but we can only get 155 days at a time
    dfs = []
    for start, end in dates_period_iterator(start_date, end_date, day_span=DAYS_LIMIT):
        response_forecast = get_rte_api_response(ROUTE, start_date=format_date(start),
                                                 end_date=format_date(end))

        df = forecast_response_to_df(response_forecast)
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_forecast = clean_forecast_data(df_concat)

    return df_forecast
