import requests
import json
from utils.queue_utils import get_hash
import pandas as pd
import time

session = requests.Session()

# Utility function to fix JSON columns, ensuring lists are serialized properly


def fix_json_column(column_value):
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

request_headers = {
    'authority': 'public-api.goldstandard.org',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
    'origin': 'https://registry.goldstandard.org',
    'referer': 'https://registry.goldstandard.org/',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

request_base_params = {
    'query': '',
    'size': '25',
    'sortColumn': '',
    'sortDirection': '',
}

all_proj_dfs = []

# Loop through multiple pages of project data
for page_num in range(1, 30):
    request_params = request_base_params.copy()
    request_params['page'] = str(page_num)

    response = session.get(
        'https://public-api.goldstandard.org/projects', params=request_params, headers=request_headers)

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
    time.sleep(5)  # Add delay between requests to avoid overloading the server

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
    '/home/shubhangi.bhatia/Desktop/calyx/main_project_details.csv', index=False)

# Loop through SustainCert URLs and download project files
for url in final_proj_df['sustaincert_url']:
    project_id = url.split('/')[-1].replace('"]', '')

    url_headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
        'Connection': 'keep-alive',
        'Origin': 'https://platform.sustain-cert.com',
        'Referer': 'https://platform.sustain-cert.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'request-id': '|881c844b10d840ce91acc0d5dcb5de72.1136db9413a94074',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'traceparent': '00-881c844b10d840ce91acc0d5dcb5de72-1136db9413a94074-01',
    }

    url_params = {
        'projectID': project_id,
    }

    try:
        url_response = session.get(
            'https://sc-platform-certification-prod.azurewebsites.net/api/document/publiclist',
            params=url_params,
            headers=url_headers,
        )
        response_data = json.loads(url_response.text)
        file_names = [file['fileName'] for file in response_data['files']]

        if len(file_names) == 0:
            print('Missing public files for this registry')
            continue

        for file in file_names:
            try:
                # file_headers = {
                #     'Accept': '*/*',
                #     'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
                #     'Connection': 'keep-alive',
                #     'Origin': 'https://platform.sustain-cert.com',
                #     'Referer': 'https://platform.sustain-cert.com/',
                #     'Sec-Fetch-Dest': 'empty',
                #     'Sec-Fetch-Mode': 'cors',
                #     'Sec-Fetch-Site': 'cross-site',
                #     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                #     'request-id': '|cc8e3d238dc84b988d588a04f8590a97.2c3d845b51414477',
                #     'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                #     'sec-ch-ua-mobile': '?0',
                #     'sec-ch-ua-platform': '"Linux"',
                #     'traceparent': '00-cc8e3d238dc84b988d588a04f8590a97-2c3d845b51414477-01',
                # }
                file_headers = url_headers.copy()
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
                file_path = f'/home/shubhangi.bhatia/Desktop/calyx/project_files/{file}'
                if extension in ['pdf', 'docx', 'xlsx']:
                    with open(file_path, 'wb') as f:
                        f.write(file_response.content)
                elif extension == 'csv':
                    with open(file_path, 'w', newline='', encoding='utf-8') as f:
                        f.write(file_response.content.decode('utf-8'))

            except Exception as e:
                print(url)
                raise
                print(f"Failed to download {file}: {e}")
                continue

    except Exception as e:
        raise
        print(f"Error downloading files for project {project_id}: {e}")
        continue
