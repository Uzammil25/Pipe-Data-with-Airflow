import gspread
from google.oauth2 import service_account
import pandas as pd
import google.auth


def load_data(**context):
    # Pull the processed DataFrame from XComs
    df = context['ti'].xcom_pull(task_ids='transform')

    # Connect to Google Sheets using credentials
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    # Load credentials from JSON file
    credentials_file_path = '/home/muzammil25/airflow/credentials2.json'
    credentials, _ = google.auth.load_credentials_from_file(credentials_file_path)

    # Authorize gspread with the loaded credentials
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open('KAI Complaints')
    worksheet = spreadsheet.worksheet('Complaints')
    
    # Convert DataFrame to list of lists
    data = df.values.tolist()
    column_names = df.columns.tolist()
    data.insert(0, column_names)
    
    # Clear existing content and append new data to the worksheet
    worksheet.clear()
    worksheet.append_rows(data)