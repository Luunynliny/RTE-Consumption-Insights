import pandas as pd
import requests

from utils import get_rte_api_response, dates_period_iterator, format_date


class RTEConsumption:
    ROUTE: str = ("https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1"
                  "/consolidated_power_consumption")
    DAYS_LIMIT: int = 155

    @staticmethod
    def __consumption_response_to_df(response: requests.Response) -> pd.DataFrame:
        """
        Convert RTE API response to pandas dataframe.

        :param response: a response from the RTE API.
        :type response: requests.Response
        :return: the resposne data formatted as a dataframe.
        :rtype: pd.DataFrame
        """
        data_json = response.json()
        return pd.DataFrame(data_json['consolidated_power_consumption'][0]['values'])

    @staticmethod
    def __clean_consumption_data(df: pd.DataFrame) -> pd.DataFrame:
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
            'value': 'consumption_value',
            'start_date': 'consumption_start_date',
            'end_date': 'consumption_end_date',
            'updated_date': 'consumption_updated_date',
            'status': 'consumption_status'
        }
        df = df.rename(columns=column_mapping)

        consumption_primary_key = ['consumption_start_date']
        df_clean = df.drop_duplicates(consumption_primary_key)

        return df_clean

    @staticmethod
    def get_consumption_data(start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get the power consumption data from the RTE.

        :param start_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
        :type start_date: str
        :param end_date: a date string in ISO 8601 format YYYY-MM-DDTHH:MM:SS±hh:mm.
        :type end_date: str
        :return: a pandas dataframe of the RTE consumption data between two dates.
        :rtype: pd.DataFrame
        """
        dfs = []

        for start, end in dates_period_iterator(start_date, end_date, day_span=RTEConsumption.DAYS_LIMIT):
            response_consumption = get_rte_api_response(RTEConsumption.ROUTE, start_date=format_date(start),
                                                        end_date=format_date(end))

            df = RTEConsumption.__consumption_response_to_df(response_consumption)
            dfs.append(df)

        df_concat = pd.concat(dfs, ignore_index=True)
        df_consumption = RTEConsumption.__clean_consumption_data(df_concat)

        return df_consumption
