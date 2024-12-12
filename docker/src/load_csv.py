from elasticsearch import Elasticsearch, helpers
import pandas as pd
import os
import numpy as np

# Initialize Elasticsearch client
es = Elasticsearch(
    hosts=[{
        'host': 'elasticsearch', 
        'port': 9200,
        'scheme': 'http'   
    }],
    basic_auth=('elastic', os.environ.get('ELASTIC_PASSWORD'))
)

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def create_index_if_not_exists(index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print(f"Created index: {index_name}")

def load_csv_files():
    data_directory = '/app/data/'
    create_directory(data_directory)

     # Walk through all subdirectories and files in the data directory
    for root, dirs, files in os.walk(data_directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)

                # Load the CSV file with specified encoding
                try:
                    df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1')  # Adjust encoding if needed
                except UnicodeDecodeError as e:
                    print(f"Failed to read {file_path} due to encoding error: {e}")
                    continue

                # Replace NaN values with None
                df = df.replace({np.nan: None})

                folder = os.path.basename(os.path.dirname(file_path)).lower()

                index_name = f"{folder}_{os.path.splitext(file)[0].lower()}"
                print(index_name)

                if es.indices.exists(index=index_name):
                    print(f"Index '{index_name}' already exists. Skipping loading data from {file_path}.")
                    continue

                # Ensure the index exists
                create_index_if_not_exists(index_name)
                
                # Convert DataFrame to list of dictionaries
                records = df.to_dict(orient='records')

                # Prepare bulk data
                bulk_data = []
                for record in records:
                    bulk_data.append({
                        '_op_type': 'index',
                        '_index': index_name,
                        '_source': record
                    })

                # Use the bulk API to index documents
                try:
                    helpers.bulk(es, bulk_data)
                    print(f"Bulk indexed {len(bulk_data)} records into '{index_name}'.")
                except Exception as e:
                    print(f"Failed to index records from {file_path}")
                    print(f"Error: {e}")

if __name__ == "__main__":
    load_csv_files()
    print("CSV loader completed")
    
    # Query the data to verify it was indexed correctly
    indices_to_check = [
        'autoconsumo_2023_07_asturias', 'autoconsumo_2023_08_asturias', 'autoconsumo_2023_09_asturias', 'autoconsumo_2023_10_asturias',
        'autoconsumo_2023_11_asturias', 'autoconsumo_2023_12_asturias', 'autoconsumo_2024_01_asturias', 'autoconsumo_2024_02_asturias',
        'autoconsumo_2024_03_asturias', 'autoconsumo_2024_04_asturias', 'autoconsumo_2024_05_asturias', 'autoconsumo_2024_06_asturias',
        'autoconsumo_2024_07_asturias', 'autoconsumo_2024_08_asturias',
        'tarifa_2023_07_asturias', 'tarifa_2023_08_asturias', 'tarifa_2023_09_asturias', 'tarifa_2023_10_asturias',
        'tarifa_2023_11_asturias', 'tarifa_2023_12_asturias', 'tarifa_2024_01_asturias', 'tarifa_2024_02_asturias',
        'tarifa_2024_03_asturias', 'tarifa_2024_04_asturias', 'tarifa_2024_05_asturias', 'tarifa_2024_06_asturias',
        'tarifa_2024_07_asturias', 'tarifa_2024_08_asturias'
    ]

    for index_name in indices_to_check:
        try:
            res = es.search(index=index_name, body={'query': {'match_all': {}}})
            print(f"{index_name} index contains {res['hits']['total']['value']} documents.")
        except Exception as e:
            print(f"Failed to query {index_name} index: {e}")

    print("All CSV files have been processed.")
