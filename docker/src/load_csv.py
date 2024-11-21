from elasticsearch import Elasticsearch
import pandas as pd
import os

# Initialize Elasticsearch client
es = Elasticsearch(
    hosts=[{'host': 'elasticsearch', 'port': 9200}],
    http_auth=('elastic', os.environ.get('ELASTIC_PASSWORD'))
)
def load_csv_files():
    csv_directory = '/app/csv_files/'

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

    for csv_file in csv_files:
        file_path = os.path.join(csv_directory, csv_file)

        # Load the CSV file
        df = pd.read_csv(file_path, delimiter=';')  # Adjust delimiter if needed

        # Determine the index name based on the file name (optional)
        index_name = os.path.splitext(csv_file)[0].lower()

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')

        # Index the data into Elasticsearch
        for record in records:
            es.index(index=index_name, body=record)

        print(f"Indexed data from {csv_file} into index '{index_name}'.")

if __name__ == "__main__":
    
    load_csv_files()
    
    # Query the data to verify it was indexed correctly
    res = es.search(index='autoconsumo', body={'query': {'match_all': {}}})
    res = es.search(index='tarifa', body={'query': {'match_all': {}}})
    print("All CSV files have been processed.")
