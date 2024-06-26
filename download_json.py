import pandas as pd
import requests
import os

# INPUTS
# Personal API key; this is your unique key provided by BuiltWith
personal_api_key = "c0965146-5b10-4e99-8187-4c3c3af45493"
# File with list of URLs saved in same folder as this script; should have one column titled “URL”
website_list_file = "TargetSample638.csv"

# Import list of URLs
domain_list = pd.read_csv(website_list_file)

# Drop any missing data
domain_list.dropna(subset=['website'], inplace=True)

# Remove 'www.'
domain_list['URL'] = domain_list['website'].str.replace("www.", "")

# Build each API call; each call will have 16 comma-separated domains
api_call_list = []
start_val = 0
stop_val = 15
while start_val < len(domain_list):
    joined_domains = ','.join(domain_list['URL'][start_val:stop_val+1])
    api_call = f"https://api.builtwith.com/v21/api.json?KEY={personal_api_key}&LOOKUP={joined_domains}&"
    api_call_list.append(api_call)
    start_val += 16
    stop_val = min(start_val + 15, len(domain_list) - 1)

# Loop through each API call and download the file
for i, api_call in enumerate(api_call_list, 1):
    print(f"Processing API call: {i}")
    response = requests.get(api_call)
    if response.status_code != 200:
        print("Problem!!! Irregular status code.")
    else:
        with open(os.path.join('json_files_02122024', f'{i}_BW_api_call.json'), 'wb') as file:
            file.write(response.content)
