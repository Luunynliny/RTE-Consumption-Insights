from functions import get_api_response, time_iterator
import pandas as pd
import os

def production_response_to_df(response):
    data = response.json()
    df_production_type = pd.DataFrame()
    for production_type in data['actual_generations_per_production_type']:
        df = pd.DataFrame(production_type['values'])
        df['production_type'] = production_type['production_type']
        df_production_type = pd.concat([df_production_type, df])
    return df_production_type


def clean_production_data(df):
    
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
    
    production_primary_key = ['production_start_date','production_type']
    df_clean = df.drop_duplicates(production_primary_key)
    
    return df_clean
    

def get_production_data(start_date, end_date):
    
    # get the data from the API and convert it to a table, but we can only get 155 days at a time
    dfs = []
    for start, end in time_iterator(start_date, end_date, chunk_size=50):        
        response_production = get_api_response(str(start).replace(" ", "T"), str(end).replace(" ", "T"), "production")        
        df = production_response_to_df(response_production)
        dfs.append(df)
    df_concat = pd.concat(dfs, ignore_index=True)

    # clean the data
    df_production = clean_production_data(df_concat)
    
    return df_production


if __name__ == "__main__":
    
    start_date = "2019-01-01T00:00:00+01:00"
    end_date = "2020-01-01T00:00:00+01:00"
    
    df_production = get_production_data(start_date, end_date)

    # Save the dataframe to a csv file for testing purposes
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data/production_test.csv')
    df_production.to_csv(file_path, mode='w', index=False)
