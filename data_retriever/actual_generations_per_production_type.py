from get_data import get_data_rte
import pandas as pd


def get_production_type(token, start_date, end_date):
    url = ("https://digital.iservices.rte-france.com/open_api/actual_generation/v1"
           "/actual_generations_per_production_type")
    return get_data_rte(url, token, start_date, end_date)


def table_production_type(response):
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
    return df_actual_generations_per_production_type


def clean_data_production_type(df):
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    return df


def get_production_type_data(token, start_date, end_date):
    response = get_production_type(token, start_date, end_date)
    df = table_production_type(response)
    df_clean = clean_data_production_type(df)

    return df_clean