import pandas as pd
import requests
import sqlalchemy

from .utils import get_rte_api_response, format_date_pandas_to_iso8601, dates_period_iterator, \
    today_floor_date_iso_8601, get_data_retrieving_start_date

ROUTE: str = ("https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1"
              "/consolidated_power_consumption")
DAYS_LIMIT: int = 155
TABLE_NAME: str = "consumption"


def consumption_response_to_df(response: requests.Response) -> pd.DataFrame:
    """
    Convert RTE API response to pandas dataframe.

    :param response: a response from the RTE API.
    :type response: requests.Response
    :return: the resposne data formatted as a dataframe.
    :rtype: pd.DataFrame
    """
    data_json = response.json()
    return pd.DataFrame(data_json['consolidated_power_consumption'][0]['values'])


def clean_consumption_data(df: pd.DataFrame) -> pd.DataFrame:
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


def get_consumption_data(conn: sqlalchemy.engine.base.Connection) -> pd.DataFrame:
    """
    Get the power consumption data from the RTE.
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
        response_consumption = get_rte_api_response(ROUTE, start_date=format_date_pandas_to_iso8601(start),
                                                    end_date=format_date_pandas_to_iso8601(end))

        df = consumption_response_to_df(response_consumption)
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_consumption = clean_consumption_data(df_concat)

    return df_consumption
