import requests


def generate_token() -> str:
    url = "https://digital.iservices.rte-france.com/token/oauth"

    payload = ""
    headers = {
        "User-Agent": "insomnia/2023.5.8",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": "Basic N2Q5MmQ5N2QtMzEyMi00MmQxLWExYWMtN2UwMDg5NWUxMjQzOmNkZTg2YjU4LTA4ZjctNDVlOC1iMWIzLTg2ZWRiYjI4ZDgyMA=="
    }

    response = requests.request("GET", url, data=payload, headers=headers)

    return response.json()["access_token"]