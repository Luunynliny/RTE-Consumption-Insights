from get_data import get_data_rte
import pandas as pd
import os

def get_consolidated_power_consumption(start_date, end_date):
    """
    Retrieves the consolidated power consumption data from RTE's API within the given date range using the provided API token.

    Args:
        start_date (str): The start date of the data range in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the data range in the format 'YYYY-MM-DD'.

    Returns:
        list: dictionary containing the date and the power consumption.

    """
    url = "https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1/consolidated_power_consumption"
    response = get_data_rte(url, start_date, end_date)
    data_json = response.json()
    return data_json['consolidated_power_consumption'][0]['values']


def clean_data(df):
    # Convert 'updated_date' column to datetime format
    df['updated_date'] = pd.to_datetime(df['updated_date'])
    # Rename column 'value' to 'consolidated_power_consumption'
    df = df.rename(columns={'value': 'consolidated_power_consumption_value'})
    # Convert start_date column to datetime format
    df['start_date'] = pd.to_datetime(df['start_date'])
    # Convert start_date column to datetime format
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df


if __name__ == "__main__":
    
    start_date = "2021-01-01T00:00:00+01:00"
    end_date = "2021-02-01T00:00:00+01:00"

    consolidated_power_consumption = get_consolidated_power_consumption(start_date, end_date)
    df = pd.DataFrame(consolidated_power_consumption)
    df_clean = clean_data(df)

    # Get the current directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the file path
    file_path = os.path.join(current_directory, 'data/power_consumption.csv')

    # Save df to the file and overwrite if it already exists
    df_clean.to_csv(file_path, mode='w', index=False)
    
    print("test")
    
