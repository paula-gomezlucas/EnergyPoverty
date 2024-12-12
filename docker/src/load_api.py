from elasticsearch import Elasticsearch, helpers
import requests
import os
import time
from datetime import datetime, timedelta

# Initialize Elasticsearch client with retry logic
def create_es_client():
    max_retries = 5
    retry_delay = 10  # seconds

    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                hosts=[{
                    'host': 'elasticsearch',
                    'port': 9200,
                    'scheme': 'http'
                }],
                basic_auth=('elastic', os.environ.get('ELASTIC_PASSWORD'))
            )
            # Test the connection
            if es.ping():
                print("Connected to Elasticsearch")
                return es
            else:
                print("Failed to connect to Elasticsearch")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(retry_delay)
    raise Exception("Could not connect to Elasticsearch after several attempts")

es = create_es_client()

# Constants
BASE_URL = "https://apidatos.ree.es/es/datos"
start_date = datetime.strptime("2019-01-01T00:00", "%Y-%m-%dT%H:%M")
end_date = datetime.strptime("2024-08-31T23:59", "%Y-%m-%dT%H:%M")

# Categories and widgets you want to fetch
categories_widgets = {
    'balance': ['balance-electrico'],
    'generacion': ['estructura-generacion']
    # Add other categories and widgets as needed
}

# Function to generate monthly date ranges
def get_month_ranges(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        month_start = current_date.replace(day=1, hour=0, minute=0)
        if current_date.month == 12:
            month_end = current_date.replace(year=current_date.year + 1, month=1, day=1, hour=23, minute=59) - timedelta(days=1)
        else:
            month_end = current_date.replace(month=current_date.month + 1, day=1, hour=23, minute=59) - timedelta(days=1)
        # Adjust month_end if it exceeds the overall end_date
        if month_end > end_date:
            month_end = end_date
        yield month_start, month_end
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)

def get_query_params(category, widget, start_date_str, end_date_str):
    if category == 'balance' and widget == 'balance-electrico':
        return {
            'start_date': start_date_str,
            'end_date': end_date_str,
            'time_trunc': 'day',
            'geo_trunc': 'electric_system',
            'geo_limit': 'ccaa',
            'geo_ids': '11'
        }
    elif category == 'generacion' and widget == 'estructura-generacion':
        return {
            'start_date': start_date_str,
            'end_date': end_date_str,
            'time_trunc': 'day'
            # Remove geo parameters that cause the 500 error
        }
    else:
        return {
            'start_date': start_date_str,
            'end_date': end_date_str,
            'time_trunc': 'day'
        }

def fetch_and_index_data(category, widget, start_date_str, end_date_str):
    params = get_query_params(category, widget, start_date_str, end_date_str)
    query_params = '&'.join([f"{key}={value}" for key, value in params.items()])
    api_url = f"{BASE_URL}/{category}/{widget}?{query_params}"
    print(f"Fetching data from URL: {api_url}")
    response = requests.get(api_url, headers={"Accept": "application/json"})

    if response.status_code == 200:
        data = response.json()
        bulk_data = []

        # Handle different data structures based on category or widget
        if category == 'balance' and widget == 'balance-electrico':
            # Process balance data
            for item in data.get('included', []):
                attributes = item.get('attributes', {})
                content_list = attributes.get('content', [])
                for content in content_list:
                    content_attributes = content.get('attributes', {})
                    values_list = content_attributes.get('values', [])
                    for value_item in values_list:
                        doc_id = f"{category}_{widget}_{value_item.get('datetime')}_{content_attributes.get('title')}"
                        # Check if the document already exists
                        if not es.exists(index=f"{category}_{widget}".lower(), id=doc_id):
                            doc = {
                                '_op_type': 'create',
                                '_index': f"{category}_{widget}".lower(),
                                '_id': doc_id,
                                '_source': {
                                    'category': category,
                                    'widget': widget,
                                    'type': content_attributes.get('title'),
                                    'value': value_item.get('value'),
                                    'percentage': value_item.get('percentage'),
                                    'datetime': value_item.get('datetime')
                                }
                            }
                            bulk_data.append(doc)
                            # print(f"Prepared document with ID {doc_id} for bulk indexing.")
                        else:
                            # print(f"Document with ID {doc_id} already exists in '{category}_{widget}'.")
                            pass
        elif category == 'generacion' and widget == 'estructura-generacion':
            # Process generation data
            for item in data.get('included', []):
                attributes = item.get('attributes', {})
                values_list = attributes.get('values', [])
                for value_item in values_list:
                    doc_id = f"{category}_{widget}_{value_item.get('datetime')}_{attributes.get('title')}"
                    # Check if the document already exists
                    if not es.exists(index=f"{category}_{widget}".lower(), id=doc_id):
                        doc = {
                            '_op_type': 'create',
                            '_index': f"{category}_{widget}".lower(),
                            '_id': doc_id,
                            '_source': {
                                'category': category,
                                'widget': widget,
                                'type': attributes.get('title'),
                                'value': value_item.get('value'),
                                'percentage': value_item.get('percentage'),
                                'datetime': value_item.get('datetime')
                            }
                        }
                        bulk_data.append(doc)
                        print(f"Prepared document with ID {doc_id} for bulk indexing.")
                    else:
                        # print(f"Document with ID {doc_id} already exists in '{category}_{widget}'.")
                        pass
        else:
            # Default processing (if applicable)
            pass

        # Use the bulk API to index documents
        if bulk_data:
            try:
                # Execute bulk operation with 'create' op_type
                helpers.bulk(es, bulk_data, raise_on_error=False)
                print(f"Bulk indexed {len(bulk_data)} documents into '{category}_{widget}'.")
            except helpers.BulkIndexError as bulk_error:
                for error in bulk_error.errors:
                    if error.get('create', {}).get('status') == 409:
                        doc_id = error['create']['_id']
                        # print(f"Document with ID {doc_id} already exists in '{category}_{widget}'.")
                    else:
                        print(f"Error indexing document: {error}")
    else:
        print(f"Failed to fetch data for {category}/{widget}: {response.status_code}")
        print(f"Response content: {response.content}")

if __name__ == "__main__":
    for month_start, month_end in get_month_ranges(start_date, end_date):
        start_date_str = month_start.strftime("%Y-%m-%dT%H:%M")
        end_date_str = month_end.strftime("%Y-%m-%dT%H:%M")
        print(f"Processing data for {month_start.strftime('%Y-%m')}")

        for category, widgets in categories_widgets.items():
            for widget in widgets:
                fetch_and_index_data(category, widget, start_date_str, end_date_str)

    print("All data has been fetched and indexed.")