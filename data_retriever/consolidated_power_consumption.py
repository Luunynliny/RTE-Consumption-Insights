from functions import get_data_rte, time_iterator
import pandas as pd
import os

def get_consolidated_power_consumption(start_date, end_date):
    url = "https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1/consolidated_power_consumption"
    return get_data_rte(url, start_date, end_date)

def table_power_consumption(response):
    data_json = response.json()
    return pd.DataFrame(data_json['consolidated_power_consumption'][0]['values'])


def clean_data_power_consumption(df):
    # Convert 'updated_date' column to datetime format
    df['updated_date'] = pd.to_datetime(df['updated_date'], utc=True)
    # Rename column 'value' to 'consolidated_power_consumption'
    df = df.rename(columns={'value': 'consolidated_power_consumption_value'})
    # Convert start_date column to datetime format
    df['start_date'] = pd.to_datetime(df['start_date'], utc=True)
    # Convert start_date column to datetime format
    df['end_date'] = pd.to_datetime(df['end_date'], utc=True)
    return df


def get_consolidated_power_consumption_data(start_date, end_date):
    response = get_consolidated_power_consumption(start_date, end_date)
    df = table_power_consumption(response)
    df_clean = clean_data_power_consumption(df)
    return df_clean


if __name__ == "__main__":
    
    start_date = "2019-01-01T00:00:00+01:00"
    end_date = "2024-01-01T00:00:00+01:00"
    
    # on ne peut pas récupérer plus de 155 jours de données à la fois
    dfs = []  # List to store individual dataframes
    for start, end in time_iterator(start_date, end_date):
        df = get_consolidated_power_consumption_data(str(start).replace(" ", "T"), str(end).replace(" ", "T"))
        dfs.append(df)  # Append each dataframe to the list

    # Concatenate the dataframes together
    df_clean = pd.concat(dfs, ignore_index=True)

    # Keep only distinct rows in df_clean
    df_clean = df_clean.drop_duplicates()

    # df_clean = get_consolidated_power_consumption_data(start_date, end_date)

    # Get the current directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the file path
    file_path = os.path.join(current_directory, 'data/power_consumption.csv')

    # Save df to the file and overwrite if it already exists
    df_clean.to_csv(file_path, mode='w', index=False)
