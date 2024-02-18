import pandas as pd
import requests

from utils import get_rte_api_response, dates_period_iterator, format_date


class RTEProduction:
    ROUTE: str = ("https://digital.iservices.rte-france.com/open_api/actual_generation/v1"
                  "/actual_generations_per_production_type")
    DAYS_LIMIT: int = 155

    @staticmethod
    def __production_response_to_df(response: requests.Response) -> pd.DataFrame:
        """
        Convert RTE API response to pandas dataframe.

        :param response: a response from the RTE API.
        :type response: requests.Response
        :return: the resposne data formatted as a dataframe.
        :rtype: pd.DataFrame
        """
        data = response.json()
        df_production_type = pd.DataFrame()

        for production_type in data['actual_generations_per_production_type']:
            df = pd.DataFrame(production_type['values'])
            df['production_type'] = production_type['production_type']
            df_production_type = pd.concat([df_production_type, df])

        return df_production_type

    @staticmethod
    def __clean_production_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the response dataframe.

        :param df: a pandas dataframe.
        :type df: pd.Dataframe
        :return: the cleaned pandas dataframe.
        :rtype: pd.DataFrame
        """
        date_columns = ['start_date', 'end_date', 'updated_date']
        df[date_columns] = df[date_columns].apply(pd.to_datetime, utc=True)

        column_mapping = {
            'value': 'production_value',
            'start_date': 'production_start_date',
            'end_date': 'production_end_date',
            'updated_date': 'production_updated_date',
            'production_type': 'production_type'
        }
        df = df.rename(columns=column_mapping)

        production_primary_key = ['production_start_date', 'production_type']
        df_clean = df.drop_duplicates(production_primary_key)

        return df_clean

    @staticmethod
    def get_production_data(start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get the power production data from the RTE.

        :param start_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
        :type start_date: str
        :param end_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
        :type end_date: str
        :return: a pandas dataframe of the RTE consumption data between two dates.
        :rtype: pd.DataFrame
        """
        dfs = []

        for start, end in dates_period_iterator(start_date, end_date, day_span=RTEProduction.DAYS_LIMIT):
            response_production = get_rte_api_response(RTEProduction.ROUTE, start_date=format_date(start),
                                                       end_date=format_date(end))

            df = RTEProduction.__production_response_to_df(response_production)
            dfs.append(df)

        df_concat = pd.concat(dfs, ignore_index=True)
        df_production = RTEProduction.__clean_production_data(df_concat)

        return df_production
