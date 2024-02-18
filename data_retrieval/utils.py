import pandas as pd
import requests
from dotenv import dotenv_values

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


def dates_period_iterator(start_date: str, end_date: str, day_span: int = 155) -> tuple[str, str]:
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
        yield format_date(start_date), format_date(start_date + chunk)
        start_date += chunk

    yield format_date(start_date), format_date(end_date)


def format_date(date: pd.Timestamp) -> str:
    """
    Format a pandas datetime object to a string.

    :param date: a date object in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
    :type date: pd.Timestamp
    :return: a date string in ISO 8601 format YYYY-MM-DD HH:MM:SS±hh:mm.
    :rtype: str
    """
    return str(date).replace(" ", "T")
