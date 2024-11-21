from elasticsearch import Elasticsearch
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
                http_auth=('elastic', os.environ.get('ELASTIC_PASSWORD'))
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
start_date = "2019-01-01T00:00"
end_date = "2019-01-31T23:59"
query_params = f"start_date={start_date}&end_date={end_date}&time_trunc=day&geo_trunc=electric_system&geo_limit=ccaa&geo_ids=11"

# Categories and widgets you want to fetch
categories_widgets = {
    'balance': ['balance-electrico'],
    'generacion': ['estructura-generacion']
    # Add other categories and widgets as needed
}

def fetch_and_index_data(category, widget):
    api_url = f"{BASE_URL}/{category}/{widget}?{query_params}"
    response = requests.get(api_url, headers={"Accept": "application/json"})

    if response.status_code == 200:
        data = response.json()
        documents = []

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
                        doc = {
                            'category': category,
                            'widget': widget,
                            'type': content_attributes.get('title'),
                            'value': value_item.get('value'),
                            'percentage': value_item.get('percentage'),
                            'datetime': value_item.get('datetime')
                        }
                        documents.append(doc)
        elif category == 'generacion' and widget == 'estructura-generacion':
            # Process generation data
            for item in data.get('included', []):
                attributes = item.get('attributes', {})
                values_list = attributes.get('values', [])
                for value_item in values_list:
                    doc = {
                        'category': category,
                        'widget': widget,
                        'type': attributes.get('title'),
                        'value': value_item.get('value'),
                        'percentage': value_item.get('percentage'),
                        'datetime': value_item.get('datetime')
                    }
                    documents.append(doc)
        else:
            # Default processing (if applicable)
            pass

        # Index documents into Elasticsearch
        index_name = f"{category}_{widget}"
        for doc in documents:
            es.index(index=index_name.lower(), body=doc)
        print(f"Indexed {len(documents)} documents into '{index_name.lower()}'.")
    else:
        print(f"Failed to fetch data for {category}/{widget}: {response.status_code}")

if __name__ == "__main__":
    for category, widgets in categories_widgets.items():
        for widget in widgets:
            fetch_and_index_data(category, widget)
    print("All data has been fetched and indexed.")