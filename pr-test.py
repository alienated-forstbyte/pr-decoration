try:
    import pandas as pd
    import boto3
    import requests
    import base64
    import os
    import time
    from contextlib import ExitStack
    from datetime import datetime, timedelta
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Run: pip install requests pandas openpyxl")
    exit(1)

inspector2_client = boto3.client('inspector2')
BUCKET_NAME = os.getenv('BUCKET_NAME') #S3 Bucket Name
KMS_KEY = os.getenv('KMS_KEY') #KMS
SERVICE_NAME = os.getenv('SERVICE_NAME') #Service Name
SONARQUBE_URL = os.getenv('SONARQUBE_URL') #Sonar Instance URL
PROJECT_KEY = os.getenv('SERVICE_NAME') #Your Project Key
TOKEN = os.getenv('SONAR_TOKEN') #Your Project Token
WORKSPACE = os.getenv('WORKSPACE') #Bitbucket Workspace
COMMIT_ID = os.getenv('COMMIT_ID') #Commit ID

BB_USER = os.getenv('BB_USER') #Bitbucket Username
BB_APP_PASS = os.getenv('BB_APP_PASS') #Bitbucket App Password
url = (f"https://api.bitbucket.org/2.0/repositories/{WORKSPACE}/{SERVICE_NAME}/commit/{COMMIT_ID}/pullrequests")
 #Bitbucket PR Comments URL


def export():

    # Function to write data in chunks to Excel
    def write_chunk_to_excel(filename, chunk_data, mode='w'):
        df = pd.DataFrame(chunk_data)
        if mode == 'w':
            # First chunk: create new file
            df.to_excel(filename, index=False, engine='openpyxl')
        else:
            # Subsequent chunks: append to existing file without headers
            with pd.ExcelWriter(filename, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                # Get the current number of rows
                book = writer.book
                sheet = book.active
                startrow = sheet.max_row
                # Write the new data
                df.to_excel(writer, index=False, header=False, startrow=startrow)

    # Fetch issues from SonarQube
    auth = base64.b64encode(f'{TOKEN}:'.encode()).decode()
    headers = {'Authorization': f'Basic {auth}'}
    page_size = 500  # Page size, maximum allowed by SonarQube

    # Adjust date ranges as necessary to ensure each range returns less than 10,000 issues
    start_date = datetime.now()# Example start date
    end_date = datetime.now()# Current date and time
    delta = timedelta(days=7)  # Adjust the range to ensure < 10,000 results
    excel_file = f'./sonarqube-{SERVICE_NAME}.xlsx'
    current_start_date = start_date
    all_issues = []
    chunk_size = 10000  # Write to file every 5000 issues
    excel_mode = 'w'  # Start with write mode for the first chunk
    total_issues_count = 0

    while current_start_date < end_date:
        current_end_date = current_start_date + delta
        if current_end_date > end_date:
            current_end_date = end_date
            
        print(f"Fetching issues from {current_start_date.strftime('%Y-%m-%d')} to {current_end_date.strftime('%Y-%m-%d')}...")

        params = { #Adjust as required
            'componentKeys': PROJECT_KEY,
            'createdAfter': current_start_date.strftime('%Y-%m-%d'),
            'createdBefore': current_end_date.strftime('%Y-%m-%d'),
            'ps': page_size,
            'p': 1
        }

        while True:
            try:
                response = requests.get(SONARQUBE_URL, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        issues = data.get('issues', [])
                        all_issues.extend(issues)
                        total_issues_count += len(issues)
                        
                        # Write to Excel in chunks to save memory
                        if len(all_issues) >= chunk_size:
                            print(f"Writing chunk of {len(all_issues)} issues to Excel...")
                            write_chunk_to_excel(excel_file, all_issues, excel_mode)
                            all_issues = []  # Clear memory
                            excel_mode = 'a'  # Switch to append mode after first write
                        
                        # Check if there are more pages
                        if len(issues) < page_size:
                            break  # No more pages
                        else:
                            params['p'] += 1  # Next page
                    except requests.exceptions.JSONDecodeError as e:
                        print('Failed to parse JSON response:', e)
                        print('Response content:', response.text)
                        break
                else:
                    if response.status_code == 401:
                        print('❌ Authentication failed. Check your TOKEN.')
                    elif response.status_code == 404:
                        print('❌ Project not found. Check your PROJECT_KEY and SONARQUBE_URL.')
                    elif response.status_code == 403:
                        print('❌ Access denied. Check project permissions.')
                    else:
                        print(f'❌ API request failed with status {response.status_code}')
                    print('Response content:', response.text)
                    break
            except requests.exceptions.Timeout:
                print('❌ Connection timed out. Check your network or try again later.')
                break
            except requests.exceptions.ConnectionError:
                print('❌ Connection error. Check your network and SONARQUBE_URL.')
                break
            except Exception as e:
                print(f'❌ Unexpected error occurred: {e}')
                break
                
        current_start_date = current_end_date
        print(f"Found {total_issues_count} issues so far in {SERVICE_NAME}...")

    # Handle any remaining issues
    if all_issues:
        print(f"Writing final chunk of {len(all_issues)} issues to {SERVICE_NAME} Excel...")
        write_chunk_to_excel(excel_file, all_issues, excel_mode)

    if total_issues_count > 0:
        print(f'✅ Export completed: {total_issues_count} issues exported to {excel_file}')
        print(f'📊 Date range: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
    else:
        print(f'No issues found in {SERVICE_NAME}.')

def inspectorFindingsExport():
    response = inspector2_client.create_findings_report(
        filterCriteria={
            'findingStatus': [
                {
                    'comparison': 'EQUALS',
                    'value': 'ACTIVE'
                },
            ],
            'ecrImageTags': [
                {
                    'comparison': 'EQUALS',
                    'value': 'latest'
                },
            ],
            'ecrRepositoryName': [
                {
                    'comparison': 'EQUALS',
                    'value': SERVICE_NAME
                }
            ]
        },
        reportFormat='CSV',
        s3Destination={
            'bucketName': BUCKET_NAME,
            'kmsKeyArn': KMS_KEY
        }
    )

    paginator = inspector2_client.get_paginator("list_findings")
    response_iterator = paginator.paginate(
        filterCriteria={
            "ecrImageTags": [{"comparison": "EQUALS", "value": "latest"}]
        }
    )

    critical_count = 0
    high_count = 0 
    medium_count = 0
    low_count = 0
    info_count = 0
    untriaged_count = 0
    total_count = 0
    for page in response_iterator:
        findings = page.get("findings", [])
        for f in findings:
            if f.get("severity") in {"CRITICAL"}:
                critical_count += 1
            elif f.get("severity") in {"HIGH"}:
                high_count += 1
            elif f.get("severity") in {"MEDIUM"}:
                medium_count += 1
            elif f.get("severity") in {"LOW"}:
                low_count += 1
            elif f.get("severity") in {"INFORMATIONAL"}:
                info_count += 1
            elif f.get("severity") in {"UNTRIAGED"}:
                untriaged_count += 1

    # total_count = critical_count + high_count + medium_count + low_count + info_count + untriaged_count
    time.sleep(5)

    # --------------------------
    # Get report from s3
    # --------------------------
    s3 = boto3.client('s3')
    
    bucketObjects = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" not in bucketObjects:
        return {"error": "No files found in bucket"}

    latest_file = sorted(
        bucketObjects["Contents"],
        key=lambda x: x["LastModified"],
        reverse=True
    )[0]

    latest_key = latest_file["Key"]

    # file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)
    # csv_bytes = file_obj["Body"].read()
    s3.download_file(BUCKET_NAME, latest_key, f"inspector2-{SERVICE_NAME}.csv")

export()
inspectorFindingsExport()

with ExitStack() as stack:
    files = [
        ("files", (f"sonarqube-{SERVICE_NAME}.xlsx", stack.enter_context(open(f"./sonarqube-{SERVICE_NAME}.xlsx", "rb")))),
        ("files", (f"inspector2-{SERVICE_NAME}.csv", stack.enter_context(open(f"./inspector2-{SERVICE_NAME}.csv", "rb"))))
    ]

    response = requests.post(
        url,
        auth=(BB_USER, BB_APP_PASS),
        data={"content.raw": "Attached SonarQube analysis artifacts"},
        files=files,
        timeout=30
    )

response.raise_for_status()
