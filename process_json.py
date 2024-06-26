import json
import pandas as pd
import ast
import os
from datetime import datetime
from tqdm import tqdm 

def convert_timestamp_to_date(timestamp):
    if pd.isnull(timestamp) or timestamp == 0:
        return None
    
    try:
        timestamp_int = int(timestamp)
    except ValueError:
        print(f"Error converting timestamp: {timestamp}")
        return None
    
    return datetime.utcfromtimestamp(timestamp_int / 1000).strftime('%Y-%m-%d')


def convert_spend_history(spend_history):
    if isinstance(spend_history, (list, dict)):
        spend_history_list = spend_history
    elif pd.isnull(spend_history) or spend_history == '':
        return None
    else:
        try:
            spend_history_list = ast.literal_eval(spend_history)
        except ValueError as e:
            print(f"Error parsing SpendHistory: {spend_history}. Error: {e}")
            return None
    
    for entry in spend_history_list:
        if isinstance(entry, dict) and 'D' in entry:
            entry['D'] = convert_timestamp_to_date(entry['D'])
    
    return str(spend_history_list)


def split_categories(df, column_name):
    categories_df = df[column_name].str.split(';', expand=True)
    categories_df.columns = [f'Category {i+1}' for i in range(categories_df.shape[1])]
    return categories_df

def process_json_files(folder_path):
    all_data = [] 
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    
    # Iterate over JSON files with a progress bar
    for filename in tqdm(json_files, desc="Processing JSON files"):
        json_file_path = os.path.join(folder_path, filename)
        with open(json_file_path, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

            lookups = json_data['Results']
            errors = json_data['Errors']

            for lookup in lookups:
                paths_count = len(lookup['Result']['Paths'])
                for path in lookup['Result']['Paths']:
                    for technology in path['Technologies']:
                        categories = technology.get('Categories', None)
                        categories_list = "; ".join(categories) if categories is not None else ""
                        row = [
                            lookup['Lookup'], lookup['FirstIndexed'], lookup['LastIndexed'], lookup['Meta']['CompanyName'], lookup['Result']['IsDB'], lookup['Result']['Spend'], lookup['SalesRevenue'], lookup['Result']['SpendHistory'], paths_count, path['Domain'], path['Url'], path['SubDomain'], path['FirstIndexed'], path['LastIndexed'], technology['IsPremium'], technology['Name'], technology['Link'], technology['Description'], technology['Tag'], technology.get('Parent', ''), technology['FirstDetected'], technology['LastDetected'], len(categories) if categories is not None else 0, categories_list
                        ]
                        all_data.append(row)

            for error in errors:
                row = [error['Lookup'], error['Message'], error['Code']] + [None] * 21  # Fill the rest of the row with None values
                all_data.append(row)

    # Convert all_data to a DataFrame
    df = pd.DataFrame(all_data, columns=['Lookup', 'FirstIndexed', 'LastIndexed', 'CompanyName', 'IsDB', 'Spend', 'SalesRevenue', 'SpendHistory', 'paths_count', 'path_Domain', 'path_Url', "path_SubDomain", 'path_FirstIndexed', 'path_LastIndexed', 'IsPremium', 'Name', 'Link', 'Description', 'Tag', 'Parent', 'FirstDetected', 'LastDetected', 'Categories_count', 'Categories_list'])

    # Apply conversions and modifications
    for col in ['FirstIndexed', 'LastIndexed', 'path_FirstIndexed', 'path_LastIndexed', 'FirstDetected', 'LastDetected']:
        df[col] = df[col].apply(convert_timestamp_to_date)

    #df['SpendHistory'] = df['SpendHistory'].apply(convert_spend_history)

    # Split 'Categories_list' column and combine with the original DataFrame
    categories_split_df = split_categories(df, 'Categories_list')
    df_with_categories = pd.concat([df, categories_split_df], axis=1)
    df_with_categories.drop('Categories_list', axis=1, inplace=True)

    return df_with_categories

# Example usage
folder_path = './json_files_02232024'
final_df = process_json_files(folder_path)
csv_output_path = './json_files_02232024/cleaned_data.csv'
final_df.to_csv(csv_output_path, index=False)
print(f"Combined CSV file created: {csv_output_path}")
