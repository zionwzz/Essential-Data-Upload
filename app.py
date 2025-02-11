from flask import Flask, render_template_string, request, send_file
from io import BytesIO
import pandas as pd
from boxsdk import Client, OAuth2

app = Flask(__name__)

# Function to authenticate with Box
def authenticate_box_client(client_id, client_secret, developer_token):
    auth = OAuth2(client_id=client_id, client_secret=client_secret, access_token=developer_token)
    return Client(auth)

# Function to list folders in Box
def list_folders(client, folder_id):
    folder = client.get_shared_item(folder_id)
    folders = [(item.id, item.name) for item in folder.get_items() if item.type == 'folder']
    return folders

# Function to fetch and process CSV files from Box
def fetch_and_process_csv(client, shared_folder_id, patient_no):
    folders = list_folders(client, shared_folder_id)
    
    selected_folder = None
    for folder_id, folder_name in folders:
        if patient_no in folder_name:
            selected_folder = client.folder(folder_id)
            break

    if not selected_folder:
        return None, "No folder found matching the patient number."

    # Fetch SIReport files
    csv_files = [item for item in selected_folder.get_items() if item.type == 'file' and item.name.startswith('SIReport') and item.name.endswith('.csv')]
    
    if not csv_files:
        return None, "No 'SIReport' CSV files found."

    combined_data = []
    for file in csv_files:
        content = file.content().decode('utf-8')
        df = pd.read_csv(BytesIO(file.content()))  # Convert to DataFrame
        combined_data.append(df)

    final_df = pd.concat(combined_data, ignore_index=True)

    # Convert DataFrame to a binary CSV file in memory
    output = BytesIO()
    final_df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)

    return output, None

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

            <label for="patient_no">Patient Number:</label>
            <input type="text" name="patient_no" required>

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
        patient_no = request.form['patient_no']

        try:
            client = authenticate_box_client(client_id, client_secret, developer_token)
            csv_file, error = fetch_and_process_csv(client, shared_folder_id, patient_no)

            if error:
                return render_template_string(html_template, error=error)

            return send_file(
                csv_file,
                mimetype='text/csv',
                as_attachment=True,
                download_name=f"patient_{patient_no}_data.csv"
            )

        except Exception as e:
            return render_template_string(html_template, error=str(e))

    return render_template_string(html_template)

if __name__ == '__main__':
    app.run(debug=True)
