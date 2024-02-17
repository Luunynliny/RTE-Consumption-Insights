import requests
import pandas as pd


def generate_token() -> str:
    
    url = "https://digital.iservices.rte-france.com/token/oauth"
    headers = {
        "User-Agent": "insomnia/2023.5.8",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": "Basic N2Q5MmQ5N2QtMzEyMi00MmQxLWExYWMtN2UwMDg5NWUxMjQzOmNkZTg2YjU4LTA4ZjctNDVlOC1iMWIzLTg2ZWRiYjI4ZDgyMA=="
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()["access_token"]


# def get_data_rte(url, start_date, end_date):
    
#     querystring = {"start_date":f"{start_date}","end_date":f"{end_date}"}
#     headers = {"Authorization": f"Bearer {generate_token()}"}

#     try:
#         response = requests.request("GET", url, headers=headers, params=querystring)
#         response.raise_for_status()  # Raise an exception if the response status code is not successful
#         return response
#     except requests.exceptions.RequestException as e:
#         print(f"Error occurred: {e}")
#         return None
    
    
def get_api_response(start_date, end_date, route):
    
    routes = {
        "production": "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type",
        "consumption": "https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1/consolidated_power_consumption",
        "forecast": "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts"
    }

    if route not in routes:
        raise ValueError("Invalid route")

    url = routes[route]
    
    querystring = {"start_date":f"{start_date}","end_date":f"{end_date}"}
    headers = {"Authorization": f"Bearer {generate_token()}"}

    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an exception if the response status code is not successful
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None


def time_iterator(start_date, end_date, chunk_size=155):
    # on ne peut pas récupérer plus de 155 jours de données à la fois
    chunk = pd.Timedelta(chunk_size, "D")
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    while start_date + chunk < end_date:
        yield start_date, start_date + chunk
        start_date += chunk
    yield start_date, end_date