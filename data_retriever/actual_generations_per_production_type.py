from functions import get_data_rte, time_iterator
import pandas as pd
import os

def get_consolidated_production_type(start_date, end_date):
    url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"
    return get_data_rte(url, start_date, end_date)


def table_production_type(response):
    data = response.json()
    df_production_type = pd.DataFrame()
    for production_type in data['actual_generations_per_production_type']:
        df = pd.DataFrame(production_type['values'])
        df['production_type'] = production_type['production_type']
        df_production_type = pd.concat([df_production_type, df])
    return df_production_type


def clean_data_production_type(df):
    df["start_date"] = pd.to_datetime(df["start_date"], utc=True)
    df["end_date"] = pd.to_datetime(df["end_date"], utc=True)
    df = df.rename(columns={'value': 'production_value'})
    return df


def get_consolidated_production_type_data(start_date, end_date):
    response = get_consolidated_production_type(start_date, end_date)
    df = table_production_type(response)
    df_clean = clean_data_production_type(df)
    return df_clean


if __name__ == "__main__":
    
    start_date = "2019-01-01T00:00:00+01:00"
    end_date = "2024-01-01T00:00:00+01:00"
    
    # on ne peut pas récupérer plus de 155 jours de données à la fois
    dfs = []  # List to store individual dataframes
    for start, end in time_iterator(start_date, end_date, chunk_size=50):
        df = get_consolidated_production_type_data(str(start).replace(" ", "T"), str(end).replace(" ", "T"))
        dfs.append(df)  # Append each dataframe to the list

    # Concatenate the dataframes together
    df_clean = pd.concat(dfs, ignore_index=True)

    # Keep only distinct rows in df_clean
    df_clean = df_clean.drop_duplicates()

    # df_clean = get_consolidated_production_type_data(start_date, end_date)

    # Get the current directory
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Define the file path
    file_path = os.path.join(current_directory, 'data/production_type.csv')

    # Save df to the file and overwrite if it already exists
    df_clean.to_csv(file_path, mode='w', index=False)
