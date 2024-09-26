import requests
import json
import pandas as pd
import time
from typing import Sequence
import hashlib
from __init__ import InitInfo

CONST_OBJ = InitInfo()

session = requests.Session()

def get_hash(data_dict: dict,
             hsh_keys: Sequence,
             print_hsh_tup: bool = False) -> str:
    """
    Generate a hash from a dictionary, using a subset of the keys provided in
    `hsh_keys`. Useful for generating a unique identifier for a given set of
    fields.

    Parameters
    ----------
    data_dict : dict
        Input dictionary to hash.
    hsh_keys : Sequence
        Sequence of keys to use when hashing.
    print_hsh_tup : bool, optional
        If `True`, print the tuple used to generate the hash.

    Returns
    -------
    str
        Hexadecimal representation of the hash.
    """
    assert hsh_keys, "Hash keys are empty"
    tuple_ = tuple(data_dict[k] for k in hsh_keys)
    tuple_str = str(tuple_)
    if print_hsh_tup:
        print(tuple_str)
    return hashlib.md5(tuple_str.encode()).hexdigest()


def fix_json_column(column_value):
    """
    Utility function to fix JSON columns, ensuring lists are serialized properly.

    Parameters
    ----------
    column_value
        The value of the column to fix. If it is a list, it will be serialized
        into a JSON string. Otherwise, it will be returned unchanged.

    Returns
    -------
    str
        The fixed column value. If `column_value` was a list, this will be a
        JSON-serialized string. Otherwise, it will be the original value.
    """
    if isinstance(column_value, list):
        return json.dumps(column_value)
    return column_value


CLEAN_HSH_KEYS = ['sustaincert_id', 'name', 'country']

start_headers = {
    'authority': 'registry.goldstandard.org',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
    'cache-control': 'max-age=0',
    # 'cookie': '_ga=GA1.1.1245459337.1727155543; _ga_DRCFDK09LK=GS1.1.1727155543.1.1.1727155927.0.0.0',
    'if-modified-since': 'Mon, 16 Sep 2024 09:10:32 UTC',
    'if-none-match': '"adecbe62effb5549dd1472737118160a"',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}
start_params = {
    'q': '',
    'page': '1',
}
session.get(
    'https://registry.goldstandard.org/projects', params=start_params, headers=start_headers)

all_proj_dfs = []

# Loop through multiple pages of project data
for page_num in range(1, 3):
    PRIORITY: bool = False
    CLEAN_CONFIG = {
        "collection_name": 'high_court',
        "table_name": 'assam',
        "constraint_name": 'hsh_unique_as',
    }
    request_params = CONST_OBJ.request_base_params.copy()
    request_params['page'] = str(page_num)

    response = session.get(
        'https://public-api.goldstandard.org/projects', params=request_params, headers=CONST_OBJ.request_headers)

    if response.status_code != 200:
        print(
            f"Error fetching data for page {page_num}: {response.status_code}")
        continue

    # Parse JSON response
    try:
        response = json.loads(response.text)
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for page {page_num}")
        continue

    # Add a hash column based on selected keys
    for dict_item in response:
        dict_item['hsh'] = get_hash(dict_item, CLEAN_HSH_KEYS)

    proj_df = pd.DataFrame(response)
    if 'latitude' in proj_df.columns and 'longitude' in proj_df.columns:
        proj_df = proj_df.drop(['latitude', 'longitude'], axis=1)

    all_proj_dfs.append(proj_df)
    print(f'Fetched data for page {page_num}')
    time.sleep(3)  # Add delay between requests to avoid overloading the server

final_proj_df = pd.concat(all_proj_dfs, ignore_index=True)

# Fix JSON columns and apply transformations
final_proj_df['sustaincert_url'] = final_proj_df['sustaincert_url'].apply(
    lambda x: json.dumps([x]))
final_proj_df['sustainable_development_goals'] = final_proj_df['sustainable_development_goals'].apply(
    fix_json_column)

# Reorder and filter columns as necessary
final_proj_df = final_proj_df[['id', 'created_at', 'updated_at', 'name', 'description', 'status', 'gsf_standards_version', 'estimated_annual_credits', 'crediting_period_start_date', 'crediting_period_end_date', 'methodology', 'type', 'size',
                               'sustaincert_id', 'sustaincert_url', 'project_developer', 'carbon_stream', 'country', 'country_code', 'state', 'programme_of_activities', 'poa_project_id', 'poa_project_sustaincert_id', 'poa_project_name', 'sustainable_development_goals', 'labels', 'hsh']]

final_proj_df.to_csv(
    './main_project_details.csv', index=False)

# Loop through SustainCert URLs and download project files
for url in final_proj_df['sustaincert_url']:
    project_id = url.split('/')[-1].replace('"]', '')

    url_params = {
        'projectID': project_id,
    }

    try:
        url_response = session.get(
            'https://sc-platform-certification-prod.azurewebsites.net/api/document/publiclist',
            params=url_params,
            headers=CONST_OBJ.url_headers,
        )
        response_data = json.loads(url_response.text)
        file_names = [file['fileName'] for file in response_data['files']]

        if len(file_names) == 0:
            print('Missing public files for this registry')
            continue

        for file in file_names:
            try:
                file_headers = CONST_OBJ.url_headers.copy()
                file_params = {
                    'projectID': project_id,
                    'fileName': file,
                }

                file_response = session.get(
                    'https://sc-platform-certification-prod.azurewebsites.net/api/document/publicdownload',
                    params=file_params,
                    headers=file_headers,
                )

                # Save the file based on its extension
                extension = file.split('.')[-1]
                file_path = f'./project_files/{file}'
                if extension in ['pdf', 'docx', 'xlsx']:
                    with open(file_path, 'wb') as f:
                        f.write(file_response.content)
                elif extension == 'csv':
                    with open(file_path, 'w', newline='', encoding='utf-8') as f:
                        f.write(file_response.content.decode('utf-8'))

            except Exception as e:
                print(f"Failed to download {file}: {e}")
                continue

    except Exception as e:
        print(f"Error downloading files for project {project_id}: {e}")
        continue
