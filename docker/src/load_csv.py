import os
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers

# Elasticsearch client
es = Elasticsearch(["http://elasticsearch:9200"])

def delete_existing_indices(es, prefix):
    # Delete indices with specific prefixes (e.g., tarifa_*, autoconsumo_*)
    indices = es.indices.get_alias("*")
    for index in indices:
        if index.startswith(prefix):
            es.indices.delete(index=index)
            print(f"Deleted index: {index}")


# Create directory if it doesn't exist
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Create Elasticsearch index with mapping
def create_index_with_mapping(index_name, mapping):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={"mappings": mapping})

# Process a single file based on category
def process_file(file_path, category):
    print(f"Processing file: {file_path} for category: {category}")
    if category == "paro":
        df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1', header=1, index_col=False)
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('ó', 'o', regex=True)
        print("Columns before creating 'unique_id':", df.columns)
        print("DataFrame shape before creating 'unique_id':", df.shape)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['codigo_mes'].astype(str) + df['codigo_municipio'].astype(str)
        df['unique_id'] = df['unique_id'].str.strip()
    elif category == "autoconsumo":
        df = pd.read_csv(file_path, delimiter=';', encoding='UTF-8', header=0, index_col=False)
        print("Columns before creating 'unique_id':", df.columns)
        print("DataFrame shape before creating 'unique_id':", df.shape)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['dataDate'].astype(str) + df['province'].astype(str) + df['selfConsumption'].astype(str)
        df['unique_id'] = df['unique_id'].str.strip()
    elif category == "tarifa":
        df = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1', header=0, index_col=False)
        print("Columns before creating 'unique_id':", df.columns)
        print("DataFrame shape before creating 'unique_id':", df.shape)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['dataDate'].astype(str) + df['municipality'].astype(str) + df['sector'].astype(str) + df['fare'].astype(str)
        df['unique_id'] = df['unique_id'].str.strip()
    elif category == "econ":
        df = pd.read_csv(file_path, delimiter=',', encoding='utf-8', header=0, index_col=False)
        print("Columns before creating 'unique_id':", df.columns)
        print("DataFrame shape before creating 'unique_id':", df.shape)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['Periodo'].astype(str)
        df['unique_id'] = df['unique_id'].str.strip()
    elif category == "inundacion":
        df = pd.read_csv(file_path, delimiter='|', encoding='utf-8', header=0, index_col=False)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['Cauces'] + df['Nucleos afectados'] + df['Origen de la inundacion']
        df['unique_id'] = df['unique_id'].str.strip()
    elif category == "lim_geo":
        df = pd.read_csv(file_path, delimiter='|', encoding='utf-8', header=0, index_col=False)
        df['unique_id'] = " "
        df['unique_id'] = df['unique_id'] + df['Codigo'].astype(str) + df['Unidad territorial']
        df['unique_id'] = df['unique_id'].str.strip()
    else:
        raise ValueError(f"Unknown category: {category}")
    
    if 'unique_id' not in df.columns or df['unique_id'].isnull().any():
        raise ValueError(f"'unique_id' is not properly created for file: {file_path}. Check input data and processing logic.")
    else:
        required_columns = {
            "paro": ["codigo_mes", "codigo_municipio"],
            "autoconsumo": ["dataDate", "province", "selfConsumption"],
            "tarifa": ["dataDate", "municipality", "sector", "fare"],
            "econ": ["Periodo"],
            "inundacion": ["Cauces", "Nucleos afectados", "Origen de la inundacion"],
            "lim_geo": ["Codigo", "Unidad territorial"]
        }
        missing_columns = [col for col in required_columns[category] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns for category '{category}': {missing_columns}")

        print("unique_id created for file:")
        print(file_path)
        print("Columns after creating 'unique_id':", df.columns)
        print("DataFrame shape after 'unique_id':", df.shape)
        df = df.set_index('unique_id')
        print("If you get here, unique_id SHOULD exist in the DataFrame")
        print("Sample 'unique_id' values:", df.index[:5])

    return df

# Load and combine all files in a category
def load_and_combine_csvs(category, files):
    combined_df = []
    for file_path in files:
        df = process_file(file_path, category)
        combined_df.append(df)
    if not combined_df:
        raise ValueError(f"No valid DataFrames to combine for category: {category}")
    combined_df = pd.concat(combined_df, ignore_index=True)
    return combined_df

# Main function to load CSV files
def load_csv_files():
    data_directory = '/app/data/'
    processed_files_dir = '/app/persisted_csv/'
    create_directory(processed_files_dir)

    # Predefined mappings for Elasticsearch indices
    mappings = {
        "tarifa": {
            "properties": {
                "year": {"type": "integer"},
                "month": {"type": "integer"},
                "day": {"type": "integer"},
                "community": {"type": "keyword"},
                "province": {"type": "keyword"},
                "municipality": {"type": "keyword"},
                "sector": {"type": "keyword"},
                "fare": {"type": "keyword"},
                "sumContracts": {"type": "integer"},
                "sumEnergy_kWh": {"type": "float"},
            }
        },
        "autoconsumo": {
            "properties": {
                "year": {"type": "integer"},
                "month": {"type": "integer"},
                "day": {"type": "integer"},
                "community": {"type": "keyword"},
                "province": {"type": "keyword"},
                "municipality": {"type": "keyword"},
                "selfConsumption": {"type": "keyword"},
                "sumPower_kW": {"type": "float"},
                "sumPouredEnergy_kWh": {"type": "float"},
                "sumContracts": {"type": "integer"},
            }
        },
        "paro": {
            "properties": {
                "codigo_mes": {"type": "keyword"},
                "municipio": {"type": "keyword"},
                "total Paro Registrado": {"type": "integer"},
            }
        },
        "econ": {
            "properties": {
                "Periodo": {"type": "integer"}
            }
        },
        "inundacion": {
            "properties": {
                "Cauces": {"type": "keyword"},
                "Nucleos afectados": {"type": "keyword"},
                "Origen de la inundacion": {"type": "keyword"}
            }
        },
        "lim_geo": {
            "properties": {
                "Codigo": {"type": "integer"},
                "Unidad territorial": {"type": "keyword"}
            }
        }

    }

    categories = ["tarifa", "autoconsumo", "paro", "econ", "inundacion", "lim_geo"]
    for category in categories:
        print(f"Processing category: {category}")
        files = [
            os.path.join(root, file)
            for root, _, file_list in os.walk(data_directory)
            for file in file_list
            if category in root.lower() and file.endswith(".csv")
        ]
        if not files:
            print(f"No files found for category: {category}")
            continue

        # Load and combine CSVs for the category
        print(f"Combining files for {category}")
        combined_df = load_and_combine_csvs(category, files)
        print(f"Combined DataFrame for {category} has {combined_df.shape[0]} records. Columns: {combined_df.columns}")

        print(f"DataFrame for {category}:\n{combined_df.head()}")

        # Create Elasticsearch index
        combined_index_name = f"{category}"
        print(f"Created combined index: {combined_index_name}")
        print(f"Uploaded {len(combined_df)} records to {combined_index_name}")

        es.indices.create(index=combined_index_name, body={"mappings": mappings}, ignore=400)
        bulk_data = [
            {
            "_index": combined_index_name,
            "_id": idx,
            "_source": record,
            }
            for idx, record in combined_df.reset_index().to_dict(orient="index").items()
        ]
        try:
            helpers.bulk(es, bulk_data)
            print(f"Indexed {len(bulk_data)} records into {combined_index_name}.")
        except Exception as e:
            print(f"Error indexing records for {category}: {e}")

if __name__ == "__main__":
    load_csv_files()
    print("CSV loader completed")