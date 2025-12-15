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
    # Directory to store progress
    progress_dir = '/app/persisted_api'
    if not os.path.exists(progress_dir):
        os.makedirs(progress_dir)
    
    # File to track the last processed date
    last_processed_file = f'{progress_dir}/{category}_{widget}_last_processed.txt'
    
    # Load the last processed date or set it to the start date
    if os.path.exists(last_processed_file):
        with open(last_processed_file, 'r') as file:
            last_processed = datetime.strptime(file.read().strip(), "%Y-%m-%dT%H:%M")
    else:
        last_processed = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M")
    
    # End date for processing
    final_end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M")
    
    # Loop through dates month by month
    while last_processed <= final_end_date:
        # Define the current month's range
        next_month = last_processed.replace(day=28) + timedelta(days=4)  # Ensure next month
        month_end = next_month - timedelta(days=next_month.day)  # End of the current month
        month_start_str = last_processed.strftime("%Y-%m-%dT%H:%M")
        month_end_str = min(month_end, final_end_date).strftime("%Y-%m-%dT%H:%M")
        
        # Prepare the API request parameters
        params = get_query_params(category, widget, month_start_str, month_end_str)
        query_params = '&'.join([f"{key}={value}" for key, value in params.items()])
        api_url = f"{BASE_URL}/{category}/{widget}?{query_params}"
        
        print(f"Fetching data from URL: {api_url}")
        
        # Make the API request
        response = requests.get(api_url, headers={"Accept": "application/json"})
        if response.status_code == 200:
            data = response.json()
            bulk_data = []
            
            # Process the data based on category/widget structure
            if category == 'balance' and widget == 'balance-electrico':
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
            elif category == 'generacion' and widget == 'estructura-generacion':
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
            
            # Bulk index the data
            if bulk_data:
                try:
                    helpers.bulk(es, bulk_data)
                    print(f"Indexed {len(bulk_data)} documents for {category}/{widget} from {month_start_str} to {month_end_str}.")
                except helpers.BulkIndexError as bulk_error:
                    print(f"Error indexing data: {bulk_error}")
        
        else:
            print(f"Failed to fetch data for {category}/{widget} from {month_start_str} to {month_end_str}. Status: {response.status_code}")
        
        # Update last processed date
        last_processed = month_end + timedelta(days=1)
        with open(last_processed_file, 'w') as file:
            file.write(last_processed.strftime("%Y-%m-%dT%H:%M"))

if __name__ == "__main__":
    for month_start, month_end in get_month_ranges(start_date, end_date):
        start_date_str = month_start.strftime("%Y-%m-%dT%H:%M")
        end_date_str = month_end.strftime("%Y-%m-%dT%H:%M")
        print(f"Processing data for {month_start.strftime('%Y-%m')}")

        for category, widgets in categories_widgets.items():
            for widget in widgets:
                fetch_and_index_data(category, widget, start_date_str, end_date_str)

    print("All data has been fetched and indexed.")