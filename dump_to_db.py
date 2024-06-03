from datetime import datetime, timedelta
import mysql.connector
import json
import csv
import tempfile
from airflow.models import XCom
import pandas as pd
from pandasql import sqldf

def dump_to_database(**kwargs):
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="user1",
        password="Toto12070!",
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
    for data in kwargs['ti'].xcom_pull(task_ids='extract'):
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO complaints ({keys}) VALUES ({values})"
        cursor.execute(query, tuple(data.values()))

    # Commit changes to the database
    connection.commit()

    # Define the SQL query
    query = "SELECT * FROM complaints"

    # Execute the query and fetch the results
    cursor.execute(query)
    rows = cursor.fetchall()

    # Convert the result to a DataFrame
    column_names = [i[0] for i in cursor.description]
    df = pd.DataFrame(rows, columns=column_names)

    # Save DataFrame to CSV file
    df.to_csv('data.csv', index=False)

    # Close MySQL connection
    connection.close()
