from flask import Flask, render_template_string, request, send_file
from io import BytesIO, StringIO
import pandas as pd
import re
import requests
from boxsdk import Client, OAuth2

app = Flask(__name__)

# 🔹 Replace with your GitHub or Google Drive raw CSV link
TEMPLATE_CSV_URL = "https://raw.githubusercontent.com/zionwzz/Essential-Data-Upload/refs/heads/main/ESSENTIALMiamiBaselineSurvey_ImportTemplate_2025-01-16.csv?token=GHSAT0AAAAAAC6ZNGX77SZE356YQX3THJDWZ5MA23A"

# Load template CSV
def load_template_csv():
    response = requests.get(TEMPLATE_CSV_URL)
    if response.status_code == 200:
        return pd.read_csv(BytesIO(response.content))
    else:
        raise Exception("Failed to load template CSV.")

# Authenticate with Box
def authenticate_box_client(client_id, client_secret, developer_token):
    auth = OAuth2(client_id=client_id, client_secret=client_secret, access_token=developer_token)
    return Client(auth)

# List folders in Box
def list_folders(client, folder_id):
    folder = client.get_shared_item(folder_id)
    return [(item.id, item.name) for item in folder.get_items() if item.type == 'folder']

# Navigate to specific patient folder
def navigate_to_folder(client, folders, code):
    for folder_id, folder_name in folders:
        if code in folder_name:
            return client.folder(folder_id).get()
    return None

# Fetch SIReport CSV files
def fetch_csv_files(folder):
    return [item for item in folder.get_items() if item.type == 'file' and item.name.endswith('.csv') and item.name.startswith('SIReport')]

# Combine multiple CSV files
def combine_sireport_csv_files(files):
    combined_data = [pd.read_csv(StringIO(file.content().decode('utf-8'))) for file in files]
    return pd.concat(combined_data, ignore_index=True)

# Fetch Fitbit export file
def fetch_fitbit_export_file(folder):
    for item in folder.get_items():
        if item.type == 'file' and item.name.startswith('fitbit_export') and item.name.endswith('.csv'):
            return item
    return None

# Read Fitbit file
def read_fitbit_export_file(file):
    return pd.read_csv(StringIO(file.content().decode('utf-8')), sep=";")

# Process Fitbit activity data
def process_activities_section(df):
    try:
        start_index = df[df['Body'] == 'Activities'].index[0]
        end_index = df[df['Body'] == 'Sleep'].index[0]
        activities_section = df.iloc[start_index + 1:end_index]
        column_names = activities_section['Body'].iloc[0].split(",")
        processed_rows = [[row.split(",", 1)[0]] + re.findall(r'"(.*?)"', row) for row in activities_section['Body'].iloc[1:]]
        return pd.DataFrame(processed_rows, columns=column_names)
    except ValueError as e:
        print(e)
        return None

# Process Fitbit sleep data
def process_sleep_section(df):
    try:
        start_index = df[df['Body'] == 'Sleep'].index[0]
        end_index = df[df['Body'].str.startswith('Food Log', na=False)].index[0]
        sleep_section = df.iloc[start_index + 1:end_index]
        column_names = sleep_section['Body'].iloc[0].split(",")
        processed_rows = [[row.split(",", 1)[0]] + re.findall(r'"(.*?)"', row) for row in sleep_section['Body'].iloc[1:]]
        df = pd.DataFrame(processed_rows, columns=column_names)
        df = df.sort_values(by='Start Time')
        df['complete'] = 2
        return df
    except ValueError as e:
        print(e)
        return None

# Fetch and process IQ Air data
def fetch_txt_files(folder):
    return [item for item in folder.get_items() if item.type == 'file' and item.name.endswith('.txt')]

def combine_txt_files_as_string(file_list):
    combined_content = ""
    is_first_file = True
    for file_item in file_list:
        file_content = file_item.content()
        content = file_content.decode('utf-8')
        lines = content.splitlines()
        if is_first_file:
            combined_content += "\n".join(lines) + "\n"
            is_first_file = False
        else:
            combined_content += "\n".join(lines[1:]) + "\n"
    return combined_content

def process_combined_data(combined_content, columns_to_average):
    df = pd.read_csv(StringIO(combined_content), sep=";", index_col=False)
    average_df = df.groupby("Date")[columns_to_average].mean().reset_index()
    average_df['complete'] = 2
    return average_df

# Append data to template
def append_data_to_template(empty_template, start_index, data, instrument_name, instance_start, constant_col_index=None, constant_col_value=None):
    na_rows = pd.DataFrame(index=range(len(data)), columns=empty_template.columns)
    empty_template = pd.concat([empty_template, na_rows], ignore_index=True)
    start_row = len(empty_template) - len(data)
    for col_idx, col_name in enumerate(data.columns):
        empty_template.iloc[start_row:, start_index + col_idx] = data[col_name]
    empty_template.iloc[start_row:, 1] = instrument_name
    empty_template.iloc[start_row:, 2] = range(instance_start, instance_start + len(data))
    if constant_col_index is not None and constant_col_value is not None:
        empty_template.iloc[start_row:, constant_col_index] = constant_col_value
    return empty_template

if __name__ == "__main__":

    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    DEVELOPER_TOKEN = "your_developer_token"
    SHARED_FOLDER_ID = "your_shared_folder_id"
    patient_no = "your_patient_no"

    client = authenticate_box_client(CLIENT_ID, CLIENT_SECRET, DEVELOPER_TOKEN)
    folders = list_folders(client, SHARED_FOLDER_ID)
    selected_folder = navigate_to_folder(client, folders, patient_no)

    sireport_files = fetch_csv_files(selected_folder)
    combined_df = combine_sireport_csv_files(sireport_files) if sireport_files else None

    fitbit_file = fetch_fitbit_export_file(selected_folder)
    fitbit_df = read_fitbit_export_file(fitbit_file) if fitbit_file else None

    fa = process_activities_section(fitbit_df) if fitbit_df is not None else None
    fs = process_sleep_section(fitbit_df) if fitbit_df is not None else None

    all_txt_files = fetch_txt_files(selected_folder)
    filtered_files = [file for file in all_txt_files if "AirVisual_values" in file.name]

    average_df = None
    if filtered_files:
        combined_content = combine_txt_files_as_string(filtered_files)
        columns_to_average = ["PM2_5(ug/m3)", "AQI(US)", "PM1(ug/m3)", "PM10(ug/m3)", "Temperature(F)", "Humidity(%RH)", "CO2(ppm)"]
        average_df = process_combined_data(combined_content, columns_to_average)

    template_df = load_template_csv()

    if combined_df is not None:
        template_df = append_data_to_template(template_df, template_df.columns.get_loc('patient_id'), combined_df, 'sleepimage_ring', 1)

    if fa is not None and fs is not None:
        template_df = append_data_to_template(template_df, template_df.columns.get_loc('date_fb'), fa, 'fitbit', 1)
        template_df = append_data_to_template(template_df, template_df.columns.get_loc('start_time_fitbit_dc5002'), fs, 'fitbit_f530f4', 1)

    if average_df is not None:
        template_df = append_data_to_template(template_df, template_df.columns.get_loc('date_iq'), average_df, 'iq_air', 1)

    output_filename = f"ESSENTIALMiamiBaselineSurvey_ImportTemplate_{patient_no}.csv"
    template_df.to_csv(output_filename, index=False)
    print(f"CSV saved as {output_filename}")