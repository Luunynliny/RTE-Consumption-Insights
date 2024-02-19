from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import sqlalchemy
from dotenv import dotenv_values
from sqlalchemy import inspect, select, text

SECRET = dotenv_values(".env")


def generate_rte_api_token() -> str:
    """
    Get a API token to access the RTE data.

    :return: a API token to access the RTE API.
    :rtype: str
    """
    url = "https://digital.iservices.rte-france.com/token/oauth"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {SECRET["RTE_API_ID"]}"
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()["access_token"]


def get_rte_api_response(route: str, start_date: str, end_date: str) -> requests.Response:
    """
    Get a response from the RTE API.

    :param route: an RTE API route.
    :type route: str
    :param start_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type start_date: str
    :param end_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type end_date: str
    :return: a response from the RTE API.
    :rtype: requests.Response
    """
    querystring = {"start_date": f"{start_date}", "end_date": f"{end_date}"}
    headers = {"Authorization": f"Bearer {generate_rte_api_token()}"}

    try:
        response = requests.request("GET", route, headers=headers, params=querystring)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None


def dates_period_iterator(start_date: str, end_date: str, day_span: int) -> tuple[str, str]:
    """
    Returns a serie of start and end dates dividing a period between two dates for a given day span.

    :param start_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type start_date: str
    :param end_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type end_date: str
    :param day_span:
    :type day_span: int
    :return: a pair of start and end date seperated by a given day span.
    :rtype: tuple[str, str]
    """
    chunk = pd.Timedelta(day_span, "D")

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while start_date + chunk < end_date:
        yield start_date, start_date + chunk
        start_date += chunk

    yield start_date, end_date


def format_date_pandas_to_iso8601(date: pd.Timestamp) -> str:
    """
    Format a pandas datetime object to a ISO 8061 date string.

    :param date: a date object in ISO 8601 format YYYY-MM-DD HH:MM:SS±hh:mm.
    :type date: pd.Timestamp
    :return: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :rtype: str
    """
    return str(date).replace(" ", "T")


def format_date_sql_to_iso8601(date: str) -> str:
    """
    Format a date string from SQL table to a ISO 8061 date string.

    :param date: a date str in format YYYY-MM-DD HH:MM.
    :type date: str
    :return: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :rtype: str
    """
    date = datetime.fromisoformat(date).astimezone(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%dT%H:%M:%S%z")
    return date[:-2] + ":" + date[-2:]


def today_floor_date_iso_8601() -> str:
    """
    Today ISO 8061 date string, florred to the previous hour.

    :return: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :rtype: str
    """
    date = datetime.now(tz=ZoneInfo("Europe/Paris")).replace(hour=0, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%dT%H:%M:%S%z")
    return date[:-2] + ":" + date[-2:]


def does_table_exist(table_name: str, conn: sqlalchemy.engine.base.Connection) -> bool:
    """
    Check if a table exists.

    :param table_name: name of a table.
    :type table_name: str
    :param conn: a sqlalchemy connection.
    :type conn: sqlalchemy.engine.base.Connection
    :return: whether the table exists.
    :rtype: bool
    """
    return table_name in inspect(conn).get_table_names()


def is_table_empty(table_name: str, conn: sqlalchemy.engine.base.Connection) -> bool:
    """
    Check if a table is empty.

    :param table_name: name of a table.
    :type table_name: str
    :param conn: a sqlalchemy connection.
    :type conn: sqlalchemy.engine.base.Connection
    :return: whether the table is empty.
    :rtype: bool
    """
    query = select("*").select_from(text(table_name)).limit(1)

    return conn.execute(query) is None


def get_table_most_recent_entry_date(table_name: str, conn: sqlalchemy.engine.base.Connection) -> str:
    """
    Get the most recent entry date.

    :param table_name: name of a table.
    :type table_name: str
    :param conn: a sqlalchemy connection.
    :type conn: sqlalchemy.engine.base.Connection
    :return: the most recent entry date.
    :rtype: str
    """
    column = f"{table_name}_start_date"
    query = select(text(column)).select_from(text(table_name)).order_by(text(f'{column} DESC')).limit(1)

    return str(conn.execute(query).scalar())


def get_data_retrieving_start_date(table_name: str, conn: sqlalchemy.engine.base.Connection) -> str:
    """
    Get the start date to retrieve data.

    :param table_name: name of a table.
    :type table_name: str
    :param conn: a sqlalchemy connection.
    :type conn: sqlalchemy.engine.base.Connection
    :return: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :rtype: str
    """
    today = today_floor_date_iso_8601()

    if not does_table_exist(table_name, conn=conn) or is_table_empty(table_name, conn=conn):
        year = today[:4]
        start_date = f"{int(year) - 5}-01-01" + today[10:]
    else:
        start_date = get_table_most_recent_entry_date(table_name, conn=conn)
        start_date = format_date_sql_to_iso8601(start_date)

    return start_date
