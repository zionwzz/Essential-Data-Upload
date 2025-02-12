from flask import Flask, render_template, request, send_file
from io import BytesIO, StringIO
import pandas as pd
import re
import os
from boxsdk import Client, OAuth2

app = Flask(__name__)

UPLOAD_FOLDER = "static"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

CSV_FILE_PATH = os.path.join(UPLOAD_FOLDER, "ESSENTIALMiamiBaselineSurvey_ImportTemplate_2025-01-16 5.03.06 PM.csv")

def authenticate_box_client(client_id, client_secret, developer_token):
    auth = OAuth2(client_id=client_id, client_secret=client_secret,
                  access_token=developer_token)
    return Client(auth)

def list_folders(client, folder_id):
    folder = client.get_shared_item(folder_id)
    print(f"Contents of folder '{folder.name}':")
    return [(item.id, item.name) for item in folder.get_items() if item.type == 'folder']

def match_patient_folder(folders, patient_no):
    for folder_id, folder_name in folders:
        if patient_no in folder_name:
            print(f"Matched folder: {folder_name} (ID: {folder_id})")
            return folder_id, folder_name
    print(f"No folder found matching patient_no '{patient_no}'.")
    return None, None

def navigate_and_fetch_files(client, folders, patient_no, file_keywords):
    folder_id, folder_name = match_patient_folder(folders, patient_no)
    if folder_id:
        folder = client.folder(folder_id).get()
        files_dict = {}

        for keyword in file_keywords:
            matched_files = [item for item in folder.get_items() if item.type == 'file' and keyword in item.name]
            files_dict[keyword] = matched_files
            print(f"Found {len(matched_files)} files containing '{keyword}' in folder '{folder_name}'.")

        return files_dict, folder_name

    return {}, None

def fetch_and_combine_csv(files, keyword=None):
    combined_data = []
    for file in files:
        print(f"Downloading and processing file: {file.name}")
        content = file.content().decode('utf-8')

        try:
            if "fitbit_export" in file.name:
                df = pd.read_csv(StringIO(content), sep=";")
            else:
                df = pd.read_csv(StringIO(content), sep=",")
        except Exception as e:
            print(f"Error reading {file.name}: {e}")
            continue  # Skip files that fail

        if keyword and keyword not in file.name:
            continue

        combined_data.append(df)

    return pd.concat(combined_data, ignore_index=True) if combined_data else pd.DataFrame()

def extract_section(df, start_keyword, end_keyword_prefix):
    try:
        start_index = df[df['Body'] == start_keyword].index[0] + 1
        end_index_candidates = df[df['Body'].str.startswith(end_keyword_prefix, na=False)].index
        end_index = end_index_candidates[0] if not end_index_candidates.empty else len(df)
        return df.iloc[start_index:end_index]
    except IndexError:
        print(f"Section {start_keyword} not found.")
        return pd.DataFrame()

def process_section(df, start_keyword, end_keyword_prefix, sort_by=None):
    section = extract_section(df, start_keyword, end_keyword_prefix)
    if section.empty:
        return section

    column_names = section.iloc[0]['Body'].split(",")
    processed_data = [[row.split(",", 1)[0]] + re.findall(r'"(.*?)"', row) for row in section['Body'].iloc[1:]]

    processed_df = pd.DataFrame(processed_data, columns=column_names)
    if sort_by:
        processed_df = processed_df.sort_values(by=sort_by)

    return processed_df

def fetch_and_process_txt(files):
    combined_content = ""
    is_first_file = True

    for file_item in files:
        print(f"Processing file: {file_item.name}")
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
    df.reset_index(drop=True, inplace=True)
    return df.groupby("Date")[columns_to_average].mean().reset_index()

def append_data_to_template(template, start_index, data, instrument_name, instance_start, constant_col_index=None, constant_col_value=None):
    empty_rows = pd.DataFrame(index=range(len(data)), columns=template.columns)
    template = pd.concat([template, empty_rows], ignore_index=True)
    start_row = len(template) - len(data)

    for col_idx, col_name in enumerate(data.columns):
        template.iloc[start_row:, start_index + col_idx] = data[col_name]

    template.iloc[start_row:, 1] = instrument_name
    template.iloc[start_row:, 2] = range(instance_start, instance_start + len(data))
    if constant_col_index is not None and constant_col_value is not None:
        template.iloc[start_row:, constant_col_index] = constant_col_value

    return template

def process_data(CLIENT_ID, CLIENT_SECRET, DEVELOPER_TOKEN, SHARED_FOLDER_ID, PATIENT_NO):
    client = authenticate_box_client(CLIENT_ID, CLIENT_SECRET, DEVELOPER_TOKEN)
    folders = list_folders(client, SHARED_FOLDER_ID)
    files_dict, folder_name = navigate_and_fetch_files(client, folders, PATIENT_NO, ["SIReport", "fitbit_export", "AirVisual_values"])

    template = pd.read_csv(CSV_FILE_PATH)

    empty_template = pd.DataFrame(columns=template.columns)

    combined_df = fetch_and_combine_csv(files_dict.get("SIReport", []))
    if not combined_df.empty:
        combined_df['complete'] = 2
        start_index = template.columns.get_loc('patient_id')
        empty_template = append_data_to_template(empty_template, start_index, combined_df, 'sleepimage_ring', 1)

    fitbit_df = fetch_and_combine_csv(files_dict.get("fitbit_export", []))
    fa = process_section(fitbit_df, "Activities", "Sleep")
    fs = process_section(fitbit_df, "Sleep", "Food Log", sort_by="Start Time")

    if not fa.empty and not fs.empty:
        start_index = template.columns.get_loc('date_fb')
        empty_template = append_data_to_template(empty_template, start_index, fa, 'fitbit', 1, constant_col_index=613, constant_col_value=2)

        start_index = template.columns.get_loc('start_time_fitbit_dc5002')
        empty_template = append_data_to_template(empty_template, start_index, fs, 'fitbit_f530f4', 1, constant_col_index=623, constant_col_value=2)

    combined_content = fetch_and_process_txt(files_dict.get("AirVisual_values", []))
    columns_to_average = ["PM2_5(ug/m3)", "AQI(US)", "PM1(ug/m3)", "PM10(ug/m3)", "Temperature(F)", "Humidity(%RH)", "CO2(ppm)"]
    average_df = process_combined_data(combined_content, columns_to_average) if combined_content else None

    if average_df is not None:
        start_index = template.columns.get_loc('date_iq')
        empty_template = append_data_to_template(empty_template, start_index, average_df, 'iq_air', 1, constant_col_index=632, constant_col_value=2)

    output_file = os.path.join(UPLOAD_FOLDER, f"ESSENTIALMiamiBaselineSurvey_ImportTemplate_{folder_name}.csv")
    empty_template.iloc[:, :-1].to_csv(output_file, index=False)
    return output_file

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        client_id = request.form['client_id']
        client_secret = request.form['client_secret']
        developer_token = request.form['developer_token']
        shared_folder_id = request.form['shared_folder_id']
        patient_no = request.form['patient_no']

        output_file, folder_name = process_data(client_id, client_secret, developer_token, shared_folder_id, patient_no)
        return render_template("index.html", file_ready=True, folder_name=folder_name)

    return render_template("index.html", file_ready=False)

@app.route('/download/<folder_name>')
def download_file(folder_name):
    file_path = os.path.join(UPLOAD_FOLDER, f"ESSENTIALMiamiBaselineSurvey_ImportTemplate_{folder_name}.csv")
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
