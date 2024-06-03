from datetime import datetime, timedelta
import pandas as pd
import mysql.connector


def transform_data():
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="user1",
        password="Toto12070!",
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

    # Convert 'date_received' column to datetime
    df['date_received'] = pd.to_datetime(df['date_received'])

    # Extract month and year from the date column and create a new column with month-year format
    df['month_year'] = df['date_received'].dt.strftime('%B %Y')

    # Group by specified columns and aggregate the count of distinct complaint IDs
    df = df.groupby(['product', 'sub_product', 'issue', 'sub_issue', 'submitted_via', 'company', 'month_year', 'state', 'timely', 'company_response'])['complaint_id'].nunique().reset_index(name='number_of_complaints')

    # Push the processed DataFrame to XComs
    return df

