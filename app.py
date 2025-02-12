from flask import Flask, render_template_string, request, send_file
from io import BytesIO, StringIO
import pandas as pd
import re
from boxsdk import Client, OAuth2

app = Flask(__name__)

TEMPLATE_CSV_URL = "https://raw.githubusercontent.com/zionwzz/Essential-Data-Upload/refs/heads/main/ESSENTIALMiamiBaselineSurvey_ImportTemplate_2025-01-16.csv"

def load_template_csv():
    response = requests.get(TEMPLATE_CSV_URL)
    if response.status_code == 200:
        return pd.read_csv(BytesIO(response.content))
    else:
        raise Exception("Failed to load template CSV.")

def authenticate_box_client(client_id, client_secret, developer_token):
    auth = OAuth2(client_id=client_id, client_secret=client_secret, access_token=developer_token)
    return Client(auth)

def list_folders(client, folder_id):
    folder = client.get_shared_item(folder_id)
    folders = [(item.id, item.name) for item in folder.get_items() if item.type == 'folder']
    return folders

def navigate_to_folder(folders, code):
    for folder_id, folder_name in folders:
        if code in folder_name:
            print(f"Navigating to folder: {folder_name} (ID: {folder_id})")
            return client.folder(folder_id).get()
    print(f"No folder found containing code '{code}'.")
    return None

def fetch_csv_files(folder):
    print(f"Fetching CSV files from folder: {folder.name}")
    all_csv_files = []

    for item in folder.get_items():
        if item.type == 'file' and item.name.endswith('.csv') and item.name.startswith('SIReport'):
            print(f"Found CSV file: {item.name}")
            all_csv_files.append(item)

    return all_csv_files

def combine_sireport_csv_files(files):
    combined_data = []

    for file in files:
        print(f"Downloading and processing file: {file.name}")
        content = file.content().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        combined_data.append(df)

    combined_df = pd.concat(combined_data, ignore_index=True)
    return combined_df

def fetch_fitbit_export_file(folder):
    print(f"Accessing shared folder: {folder.name}")

    for item in folder.get_items():
        if item.type == 'file' and item.name.startswith('fitbit_export') and item.name.endswith('.csv'):
            print(f"Found file: {item.name}")
            return item

    print("No file starting with 'fitbit_export' was found.")
    return None

def read_fitbit_export_file(file):
    print(f"Reading file: {file.name}")
    content = file.content()

    df = pd.read_csv(StringIO(content.decode('utf-8')), sep=";")
    return df

def extract_activities_section(df):
    start_index = df[df['Body'] == 'Activities'].index[0]
    end_index = df[df['Body'] == 'Sleep'].index[0]

    activities_section = df.iloc[start_index + 1:end_index]
    return activities_section

def extract_sleep_section(df):
    start_index = df[df['Body'] == 'Sleep'].index[0]
    end_index = df[df['Body'].str.startswith('Food Log', na=False)].index[0]

    sleep_section = df.iloc[start_index + 1:end_index]
    return sleep_section

def process_activities_section(df):
    try:
        activities_section = extract_activities_section(df)

        processed_rows = []

        column_names = activities_section['Body'].iloc[0].split(",")

        for row in activities_section['Body'].iloc[1:]:
            date_part = row.split(",", 1)[0]
            rest_part = re.findall(r'"(.*?)"', row)
            processed_row = [date_part] + rest_part
            processed_rows.append(processed_row)

        processed_df = pd.DataFrame(processed_rows, columns=column_names)
        output_csv_cleaned = "processed_activities_cleaned.csv"
        processed_df.to_csv(output_csv_cleaned, index=False)

        return processed_df

    except ValueError as e:
        print(e)
        return None, None

def process_sleep_section(df):
    try:
        sleep_section = extract_sleep_section(df)

        processed_rows = []

        column_names = sleep_section['Body'].iloc[0].split(",")

        for row in sleep_section['Body'].iloc[1:]:
            date_part = row.split(",", 1)[0]
            rest_part = re.findall(r'"(.*?)"', row)
            processed_row = [date_part] + rest_part
            processed_rows.append(processed_row)

        processed_df = pd.DataFrame(processed_rows, columns=column_names)
        processed_df = processed_df.sort_values(by='Start Time')
        processed_df['complete'] = 2

        return processed_df

    except ValueError as e:
        print(e)
        return None

def fetch_txt_files(folder):
    print(f"Accessing shared folder: {folder.name}")
    all_txt_files = []

    for item in folder.get_items():
        if item.type == 'file' and item.name.endswith('.txt'):
            print(f"Found text file: {item.name}")
            all_txt_files.append(item)
        elif item.type == 'folder':
            print("Note: Nested folders are not supported via shared links.")
    return all_txt_files

def combine_txt_files_as_string(file_list):
    combined_content = ""
    is_first_file = True

    for file_item in file_list:
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
    df = pd.read_csv(StringIO(combined_content), sep=";", index_col = False)
    average_df = df.groupby("Date")[columns_to_average].mean().reset_index()
    average_df['complete'] = 2
    return average_df

def filter_files_by_name(files, keyword):
    return [file for file in files if keyword in file.name]

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

def process_patient_data(patient_id, client_id, client_secret, developer_token, shared_folder_id):
    CLIENT_ID = client_id
    CLIENT_SECRET = client_secret
    DEVELOPER_TOKEN = developer_token
    SHARED_FOLDER_ID = shared_folder_id
    
    client = authenticate_box_client(CLIENT_ID, CLIENT_SECRET, DEVELOPER_TOKEN)
    folders = list_folders(client, SHARED_FOLDER_ID)
    selected_folder = navigate_to_folder(folders, patient_id)
    
    if not selected_folder:
        return {"error": "No folder found with the specified code."}

    all_txt_files = fetch_txt_files(selected_folder)
    filtered_files = filter_files_by_name(all_txt_files, "AirVisual_values")
    
    if filtered_files:
        combined_content = combine_txt_files_as_string(filtered_files)
        columns_to_average = [
            "PM2_5(ug/m3)", "AQI(US)", "PM1(ug/m3)",
            "PM10(ug/m3)", "Temperature(F)", "Humidity(%RH)", "CO2(ppm)"
        ]
        average_df = process_combined_data(combined_content, columns_to_average)
    else:
        average_df = None
    
    sireport_files = fetch_csv_files(selected_folder)
    combined_df = combine_sireport_csv_files(sireport_files) if sireport_files else None
    
    fitbit_file = fetch_fitbit_export_file(selected_folder)
    fitbit_df = read_fitbit_export_file(fitbit_file) if fitbit_file else None
    
    fa = process_activities_section(fitbit_df) if fitbit_df is not None else None
    fs = process_sleep_section(fitbit_df) if fitbit_df is not None else None
    
    template = load_template_csv()
    empty_template = pd.DataFrame(columns=template.columns)
    
    if combined_df is not None:
        start_index = template.columns.get_loc('patient_id')
        empty_template = pd.DataFrame(columns=template.columns, index=range(len(combined_df)))
        for col_idx, col_name in enumerate(combined_df.columns):
            empty_template.iloc[:, start_index + col_idx] = combined_df[col_name]
        empty_template.iloc[:, 1] = 'sleepimage_ring'
        empty_template.iloc[:, 2] = range(1, len(combined_df) + 1)
    
    if fa is not None and fs is not None:
        start_index = template.columns.get_loc('date_fb')
        empty_template = append_data_to_template(empty_template, start_index, fa, 'fitbit', 1, constant_col_index=613, constant_col_value=2)
        start_index = template.columns.get_loc('start_time_fitbit_dc5002')
        empty_template = append_data_to_template(empty_template, start_index, fs, 'fitbit_f530f4', 1)
    
    if average_df is not None:
        start_index = template.columns.get_loc('date_iq')
        empty_template = append_data_to_template(empty_template, start_index, average_df, 'iq_air', 1)
    
    empty_template.iloc[:,0] = re.search(r'\d+_+[A-Z]+_+[A-Z]+', selected_folder.name).group(0)
    output_filename = 'processed_data.csv'
    empty_template.iloc[:,:-1].to_csv(output_filename, index=False)
    
    return output_filename

# Web Interface
@app.route('/', methods=['GET', 'POST'])
def home():
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CSV Generator</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                text-align: center;
                margin: 50px;
            }
            form {
                display: inline-block;
                text-align: left;
            }
            input, button {
                width: 100%;
                padding: 10px;
                margin: 5px 0;
                font-size: 16px;
            }
        </style>
    </head>
    <body>
        <h1>Generate Patient CSV</h1>
        <p>Enter Box API credentials and patient number to generate the CSV file.</p>

        <form method="POST">
            <label for="client_id">Client ID:</label>
            <input type="text" name="client_id" required>

            <label for="client_secret">Client Secret:</label>
            <input type="text" name="client_secret" required>

            <label for="developer_token">Developer Token:</label>
            <input type="text" name="developer_token" required>

            <label for="shared_folder_id">Shared Folder ID:</label>
            <input type="text" name="shared_folder_id" required>

            <label for="patient_id">Patient Number:</label>
            <input type="text" name="patient_id" required>

            <button type="submit">Generate & Download CSV</button>
        </form>

        {% if error %}
            <p style="color: red;">{{ error }}</p>
        {% endif %}
    </body>
    </html>
    """

    if request.method == 'POST':
        client_id = request.form['client_id']
        client_secret = request.form['client_secret']
        developer_token = request.form['developer_token']
        shared_folder_id = request.form['shared_folder_id']
        patient_id = request.form['patient_id']

        output_filename = process_patient_data(patient_id, client_id, client_secret, developer_token, shared_folder_id)

    if os.path.exists(output_filename):
        return send_file(output_filename, as_attachment=True)
    else:
        return "Error: File could not be generated."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)