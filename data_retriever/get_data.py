import requests


def get_data_rte(token, url, start_date, end_date):
    """
    Retrieves data from the specified URL within the given date range using the provided API token.

    Args:
        url (str): The URL to retrieve data from.
        token (str): The API token to RTE data.
        start_date (str): The start date of the data range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the data range in the format 'YYYY-MM-DD'.

    Returns:
        requests.Response: The response object containing the retrieved data.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.

    """
    querystring = {"start_date": f"{start_date}", "end_date": f"{end_date}"}
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an exception if the response status code is not successful
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None
