import requests

def generate_token() -> str:
    
    url = "https://digital.iservices.rte-france.com/token/oauth"
    headers = {
        "User-Agent": "insomnia/2023.5.8",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": "Basic N2Q5MmQ5N2QtMzEyMi00MmQxLWExYWMtN2UwMDg5NWUxMjQzOmNkZTg2YjU4LTA4ZjctNDVlOC1iMWIzLTg2ZWRiYjI4ZDgyMA=="
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()["access_token"]

def get_data_rte(url, start_date, end_date):
    """
    Retrieves data from the specified URL within the given date range using the provided API token.

    Args:
        url (str): The URL to retrieve data from.
        start_date (str): The start date of the data range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the data range in the format 'YYYY-MM-DD'.

    Returns:
        requests.Response: The response object containing the retrieved data.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the request.

    """
    querystring = {"start_date":f"{start_date}","end_date":f"{end_date}"}
    headers = {"Authorization": f"Bearer {generate_token()}"}

    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an exception if the response status code is not successful
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None
