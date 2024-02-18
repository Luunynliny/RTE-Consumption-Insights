from dotenv import dotenv_values
from sqlalchemy import create_engine

from rte_data import consumption, production, forecast

SECRET = dotenv_values(".env")


def etl():
    start_date = "2023-01-01T00:00:00+01:00"
    end_date = "2023-01-10T00:00:00+01:00"

    df_c = consumption.get_consumption_data(start_date, end_date)
    df_p = production.get_production_data(start_date, end_date)
    df_f = forecast.get_forecast_data(start_date, end_date)

    return df_c, df_p, df_f

    # engine = create_engine(
    #     "mysql+pymysql://{}:{}@{}:{}/{}"
    #     .format(SECRET["AWS_RDS_USERNAME"],
    #             SECRET["AWS_RDS_PASSWORD"],
    #             SECRET["AWS_RDS_HOST"],
    #             SECRET["AWS_RDS_PORT"],
    #             SECRET["AWS_RDS_DATABASE"]))
    #
    # df_c.to_sql('consumption', con=engine, if_exists='append', chunksize=1000)


if __name__ == "__main__":
    c, p, f = etl()

    print(c.head())
    print(p.head())
    print(f.head())