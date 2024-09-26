import requests
import json
import pandas as pd
import time
import hashlib
import os
from typing import Sequence
from __init__ import InitInfo

CONST_OBJ = InitInfo()
session = requests.Session()


def get_hash(data_dict: dict, hsh_keys: Sequence, print_hsh_tup: bool = False) -> str:
    """Generate a hash from a dictionary."""
    assert hsh_keys, "Hash keys are empty"
    tuple_ = tuple(data_dict[k] for k in hsh_keys)
    tuple_str = str(tuple_)
    if print_hsh_tup:
        print(tuple_str)
    return hashlib.md5(tuple_str.encode()).hexdigest()


def fix_json_column(column_value):
    """Ensure lists are serialized properly."""
    if isinstance(column_value, list):
        return json.dumps(column_value)
    return column_value


def fetch_data(url: str, params: dict, headers: dict) -> list:
    """Fetch data from the API."""
    response = session.get(url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return []
    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        print("Failed to parse JSON")
        return []


def process_projects(projects: list) -> pd.DataFrame:
    """Process project data into a DataFrame."""
    for dict_item in projects:
        dict_item['hsh'] = get_hash(dict_item, CONST_OBJ.CLEAN_HSH_KEYS)
    proj_df = pd.DataFrame(projects)
    proj_df = proj_df.drop(['latitude', 'longitude'], axis=1, errors='ignore')
    return proj_df


def extract_goals(final_proj_df: pd.DataFrame) -> pd.DataFrame:
    """Extract goals and their issuable products."""
    goalist = []
    ip = []
    for index, row in final_proj_df.iterrows():
        for dict in json.loads(row['sustainable_development_goals']):
            goalist.append(dict['name'])
            ip.append(dict['issuable_products'])
    goals_dict = {goal: product for goal, product in zip(goalist, ip)}

    goal_df = pd.DataFrame()
    goal_df['goal'] = goals_dict.keys()
    goal_df['product'] = goal_df['goal'].apply(lambda g: goals_dict[g])
    goal_df['product'] = goal_df['product'].apply(fix_json_column)
    goal_df['goal_id'] = goal_df['goal'].str.extract(r'(\d+):')

    return goal_df


def download_files(project_id: str):
    """Download project files from SustainCert."""
    url_params = {'projectID': project_id}
    url_response = session.get('https://sc-platform-certification-prod.azurewebsites.net/api/document/publiclist',
                               params=url_params, headers=CONST_OBJ.url_headers)
    response_data = json.loads(url_response.text)
    file_names = [file['fileName'] for file in response_data['files']]

    if not file_names:
        print(f'No public files found for project ID {project_id}.')
        return None

    output_dir = './project_files'
    os.makedirs(output_dir, exist_ok=True)
    for file in file_names:
        try:
            file_params = {'projectID': project_id, 'fileName': file}
            file_response = session.get(
                'https://sc-platform-certification-prod.azurewebsites.net/api/document/publicdownload', params=file_params, headers=CONST_OBJ.url_headers)

            # Save the file based on its extension
            extension = file.split('.')[-1]
            file_path = f'{output_dir}/{file}'
            if extension in ['pdf', 'docx', 'xlsx']:
                with open(file_path, 'wb') as f:
                    f.write(file_response.content)
            elif extension == 'csv':
                with open(file_path, 'w', newline='', encoding='utf-8') as f:
                    f.write(file_response.content.decode('utf-8'))

        except Exception as e:
            print(f"Failed to download {file}: {e}")

    return file_names


def main():
    print('Started the scraper...')
    session.get('https://registry.goldstandard.org/projects',
                params=CONST_OBJ.start_params, headers=CONST_OBJ.start_headers)
    all_proj_dfs = []

    # Loop through multiple pages of project data, The number of pages can be changed here as needed.
    for page_num in range(1, 139):
        print(f'Processing page {page_num}...')
        request_params = CONST_OBJ.request_base_params.copy()
        request_params['page'] = str(page_num)

        projects = fetch_data('https://public-api.goldstandard.org/projects',
                              request_params, CONST_OBJ.request_headers)
        if not projects:
            continue

        proj_df = process_projects(projects)
        all_proj_dfs.append(proj_df)
        time.sleep(3)

    print('Finished processing all project data.')
    final_proj_df = pd.concat(all_proj_dfs, ignore_index=True)
    final_proj_df['sustaincert_url'] = final_proj_df['sustaincert_url'].apply(
        lambda x: json.dumps([x]))
    final_proj_df['sustainable_development_goals'] = final_proj_df['sustainable_development_goals'].apply(
        fix_json_column)

    goal_df = extract_goals(final_proj_df)
    goal_df = goal_df.drop_duplicates(subset='goal_id').reset_index(drop=True)
    goal_df.to_csv('./goals.csv', index=False)

    # Download project files
    for url in final_proj_df['sustaincert_url']:
        project_id = url.split('/')[-1].replace('"]', '')
        file_names = download_files(project_id)
        if file_names:
            final_proj_df.loc[final_proj_df['sustaincert_url']
                              == url, 'file_names'] = json.dumps(file_names)
        else:
            final_proj_df.loc[final_proj_df['sustaincert_url']
                              == url, 'file_names'] = None
    final_proj_df['file_names'] = final_proj_df['file_names'].apply(
        fix_json_column)

    # Reorder and filter columns as necessary
    final_proj_df = final_proj_df[['id', 'created_at', 'updated_at', 'name', 'description', 'status', 'gsf_standards_version', 'estimated_annual_credits', 'crediting_period_start_date', 'crediting_period_end_date', 'methodology', 'type', 'size',
                                   'sustaincert_id', 'sustaincert_url', 'project_developer', 'carbon_stream', 'country', 'country_code', 'state', 'programme_of_activities', 'poa_project_id', 'poa_project_sustaincert_id', 'poa_project_name', 'sustainable_development_goals', 'labels', 'hsh', 'file_names']]
    final_proj_df.to_csv('./main_project_details.csv', index=False)


if __name__ == "__main__":
    main()
