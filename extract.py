from datetime import datetime, timedelta, date
import requests
import json


def extract_data(**kwargs):
    url_template = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'

    size = 500
    time_delta = 365
    max_date = '2022-04-30'
    min_date = (date.fromisoformat(max_date) - timedelta(days=time_delta)).strftime("%Y-%m-%d")

    response = requests.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json")
    if response.status_code == 200:
        data = response.json()
        states_list = list(data.keys())

    output_data = {}
    for state in states_list:
        url = url_template.format(size, max_date, min_date, state)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            output_data[state] = data
        else:
            print(f"Failed to fetch data for {state} from the URL")

    source_data_list = []
    for state_data in output_data.values():
        for hit in state_data['hits']['hits']:
            source_data_list.append(hit['_source'])

    kwargs['ti'].xcom_push(key="extracted_data", value=source_data_list)
    return source_data_list
