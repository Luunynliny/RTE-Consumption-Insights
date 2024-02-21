import pandas as pd
import requests
import sqlalchemy

from .utils import get_rte_api_response, dates_period_iterator, format_date_pandas_to_iso8601, \
    get_data_retrieving_start_date, today_floor_date_iso_8601

ROUTE: str = "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts"
DAYS_LIMIT: int = 100
TABLE_NAME: str = "forecasts"


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


def get_forecast_data(conn: sqlalchemy.engine.base.Connection) -> pd.DataFrame:
    """
    Get the forecast consumption data from the RTE.
    If our table is empty (or does not exist), retrieve the last 5-years data until today,
    else starts at the most recent date entry.

    :param conn: a sqlalchemy connection.
    :type conn: sqlalchemy.engine.base.Connection
    :return: a pandas dataframe of the RTE consumption data between two dates.
    :rtype: pd.DataFrame
    """
    end_date = today_floor_date_iso_8601()
    start_date = get_data_retrieving_start_date(TABLE_NAME, conn=conn)

    dfs = []

    for start, end in dates_period_iterator(start_date, end_date, day_span=DAYS_LIMIT):
        response_forecast = get_rte_api_response(ROUTE, start_date=format_date_pandas_to_iso8601(start),
                                                 end_date=format_date_pandas_to_iso8601(end))

        df = forecast_response_to_df(response_forecast)
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_forecast = clean_forecast_data(df_concat)

    return df_forecast
