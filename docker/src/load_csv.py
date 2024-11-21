# load_csv_to_es.py
import os
import pandas as pd
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch(
    os.getenv("ELASTIC_HOST"),
    basic_auth=("elastic", os.getenv("ELASTIC_PASSWORD"))
)

# Function to process CSVs from a directory
def process_csvs(directory, index_name):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path)

            # Add additional metadata (filename or source) if necessary
            for _, row in df.iterrows():
                es.index(index=index_name, document=row.to_dict())

            print(f"Uploaded {filename} to Elasticsearch.")

# Paths to directories
autoconsumo_dir = "./data/autoconsumo"
tarifa_dir = "./data/tarifa"

# Upload to Elasticsearch
process_csvs(autoconsumo_dir, "autoconsumo_data")
process_csvs(tarifa_dir, "tarifa_data")