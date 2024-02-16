import requests
from dotenv import dotenv_values
from sqlalchemy import create_engine

from actual_generations_per_production_type import get_production_type_data
from consolidated_power_consumption import get_consolidated_power_consumption_data
from weekly_forecasts import get_weekly_forecast_data

SECRET = dotenv_values(".env")


def get_token():
    url = "https://digital.iservices.rte-france.com/token/oauth"
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {SECRET["RTE_API_ID"]}"
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()["access_token"]


def etl():
    token = get_token()

    start_date = "2023-01-31T00:00:00+01:00"
    end_date = "2023-01-01T00:00:00+01:00"

    df_pt = get_production_type_data(token, start_date, end_date)
    df_cpc = get_consolidated_power_consumption_data(token, start_date, end_date)
    df_wf = get_weekly_forecast_data(token, start_date, end_date)

    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}"
        .format(SECRET["AWS_RDS_USERNAME"],
                SECRET["AWS_RDS_PASSWORD"],
                SECRET["AWS_RDS_HOST"],
                SECRET["AWS_RDS_PORT"],
                SECRET["AWS_RDS_DATABASE"]))

    df_pt.to_sql('production_type', con=engine, if_exists='append', chunksize=1000)
    df_cpc.to_sql('power_consumption', con=engine, if_exists='append', chunksize=1000)
    df_wf.to_sql('weekly_forecast', con=engine, if_exists='append', chunksize=1000)


if __name__ == "__main__":
    etl()
