
# DAG 1 - EXTRACT

import requests
import json
from datetime import date, timedelta

# Define the URL template
url_template = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'

# Define the parameters
size = 500
time_delta = 365
max_date = '2022-04-30'
min_date = (date.fromisoformat(max_date) - timedelta(days=time_delta)).strftime("%Y-%m-%d")

# Define the list of states
response = requests.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json")
if response.status_code == 200:
    # Parse JSON data
    data = response.json()
    
    # Extract the data into a list
    states_list = list(data.keys())

# Dictionary to store the output data
output_data = {}

# Iterate over each state in the states_list
for state in states_list:
    # Construct the URL for the current state
    url = url_template.format(size, max_date, min_date, state)

    # Make a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        output_data[state] = data  # Store the retrieved data for the current state
    else:
        print(f"Failed to fetch data for {state} from the URL")


source_data_list = []

# Iterate over the output_data to collect _source values
for state_data in output_data.values():
    for hit in state_data['hits']['hits']:
        source_data_list.append(hit['_source'])

# Save the collected _source values to a JSON file
with open('source_data.json', 'w') as f:
    json.dump(source_data_list, f)
    

    

####################################################################


# DAG 2 - DUMP TO DATABASE

import mysql.connector

# Connect to MySQL database
connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="complaints"
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Create the table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS complaints (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product VARCHAR(255),
    complaint_what_happened TEXT,
    date_sent_to_company DATETIME,
    issue VARCHAR(255),
    sub_product VARCHAR(255),
    zip_code VARCHAR(10),
    tags VARCHAR(255),
    has_narrative BOOLEAN,
    complaint_id VARCHAR(255),
    timely VARCHAR(10),
    consumer_consent_provided VARCHAR(20),
    company_response VARCHAR(255),
    submitted_via VARCHAR(50),
    company VARCHAR(255),
    date_received DATETIME,
    state VARCHAR(2),
    consumer_disputed VARCHAR(10),
    company_public_response VARCHAR(255),
    sub_issue VARCHAR(255)
);
""")

# Insert data into the table
for data in source_data_list:
    keys = ', '.join(data.keys())
    values = ', '.join(['%s'] * len(data))
    query = f"INSERT INTO complaints ({keys}) VALUES ({values})"
    cursor.execute(query, tuple(data.values()))

# Commit changes to the database
connection.commit()

# Extract data from the MySQL table to a .sql file
select_query = "SELECT * FROM complaints"
cursor.execute(select_query)

with open("output.sql", "w") as f:
    rows = cursor.fetchall()

    for row in rows:
        f.write("(" + ', '.join(map(repr, row)) + "),\n")

# Commit changes to the database
connection.commit()

# Close cursor and connection
cursor.close()
connection.close()


##########################################################################


# DAG 3 - TRANSFORM

import mysql.connector
import pandas as pd

# Connect to MySQL database
connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="complaints"
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# Execute the query to fetch data into a DataFrame
select_query = "SELECT * FROM complaints"
cursor.execute(select_query)

# Fetch all rows
rows = cursor.fetchall()

# Get column names from the cursor description
columns = [col[0] for col in cursor.description]

# Create DataFrame
df = pd.DataFrame(rows, columns=columns)

# Close cursor and connection
cursor.close()
connection.close()

df['date_received'] = pd.to_datetime(df['date_received'])

# Extract month and year from the date column and create a new column with month-year format
df['month_year'] = df['date_received'].dt.strftime('%B %Y')

# Group by 'state' column and aggregate the count of distinct complaint IDs
df = df.groupby(['product','sub_product','issue','sub_issue','submitted_via','company','month_year','state','timely','company_response'])['complaint_id'].nunique()

# Convert the resulting Series to a DataFrame
df = df.reset_index(name='number_of_complaints')

# Save to csv file for record
df.to_csv('data.csv', index=False)


##########################################################################

# DAG 4 - LOAD

import gspread
from google.oauth2 import service_account
import pandas as pd

# Define the scope (replace with the required scope)
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Load credentials from the service account JSON key file and specify the required scope
credentials = service_account.Credentials.from_service_account_file(
    'credentials.json', scopes=scope
)

# Authenticate with Google Sheets using the credentials
gc = gspread.authorize(credentials)

# Open the Google Sheets spreadsheet (replace 'Your Spreadsheet Name' with your actual spreadsheet name)
spreadsheet = gc.open('Your Spreadsheet Name')

# Select the worksheet (replace 'Complaints' with your actual worksheet name)
worksheet = spreadsheet.worksheet('Complaints')

# Assuming df is your DataFrame and it contains the data you want to load to Google Sheets
# Convert the DataFrame to a list of lists
data = df.values.tolist()

# Insert column names as the first row of data
column_names = df.columns.tolist()
data.insert(0, column_names)

# Clear existing content in the worksheet
worksheet.clear()

# Append the data to the worksheet
worksheet.append_rows(data)

print("Data has been loaded into Google Sheets successfully.")
