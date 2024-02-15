import pandas as pd
import requests

from generate_api_token import generate_token

url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"

querystring = {"start_date": "2016-01-12T00:00:00+01:00", "end_date": "2016-01-13T00:00:00+01:00"}

headers = {
    "Authorization": f"Bearer {generate_token()}"
}

response = requests.request("GET", url, headers=headers, params=querystring)

data = response.json()["actual_generations_per_production_type"]

df_actual_generations_per_production_type = pd.DataFrame()

for element in data:
    production_type = element["production_type"]

    df = pd.json_normalize(element['values'], sep='_')[['start_date', 'end_date', 'value']]
    df.columns = ['start_date', 'end_date', production_type.lower()]

    df_actual_generations_per_production_type = pd.concat([df_actual_generations_per_production_type,
                                                          df.loc[
                                                            :,
                                                            ~df.columns.isin(
                                                                df_actual_generations_per_production_type.columns
                                                            )
                                                          ]],
                                                          axis=1)


df_actual_generations_per_production_type["start_date"] = pd.to_datetime(df_actual_generations_per_production_type["start_date"], format="%Y-%m-%dT%H:%M:%S%z")
df_actual_generations_per_production_type["end_date"] = pd.to_datetime(df_actual_generations_per_production_type["end_date"], format="%Y-%m-%dT%H:%M:%S%z")

print(df_actual_generations_per_production_type.head())
