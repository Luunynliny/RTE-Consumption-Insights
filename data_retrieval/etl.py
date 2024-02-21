from dotenv import dotenv_values
from sqlalchemy import create_engine
from rte_data import consumption, production, forecast

SECRET = dotenv_values(".env")


def etl():
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}"
        .format(SECRET["AWS_RDS_USERNAME"],
                SECRET["AWS_RDS_PASSWORD"],
                SECRET["AWS_RDS_HOST"],
                SECRET["AWS_RDS_PORT"],
                SECRET["AWS_RDS_DATABASE"]))

    with engine.connect() as conn:
        df_c = consumption.get_consumption_data(conn=conn)
        df_p = production.get_production_data(conn=conn)
        df_f = forecast.get_forecast_data(conn=conn)

        df_c.to_sql('consumption', con=engine, if_exists='append', chunksize=1000)
        df_p.to_sql('production', con=engine, if_exists='append', chunksize=1000)
        df_f.to_sql('forecast', con=engine, if_exists='append', chunksize=1000)


if __name__ == "__main__":
    etl()
