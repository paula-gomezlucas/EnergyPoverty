# load_api_to_es.py
import time
import requests
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

es = Elasticsearch(
    os.getenv("ELASTIC_HOST"),
    basic_auth=("elastic", os.getenv("ELASTIC_PASSWORD"))
)

BASE_URL = "https://apidatos.ree.es/es/datos"  # Spanish language
CATEGORY = "balance"
WIDGET = "balance-electrico"
GEO_PARAMS = "&geo_trunc=electric_system&geo_limit=ccaa&geo_ids=11"

# Calculate date range (last year till today)
start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00")
end_date = datetime.now().strftime("%Y-%m-%dT23:59")
TIME_AGGREGATION = "day"  # Daily data

# Build the API URL
api_url = f"{BASE_URL}/{CATEGORY}/{WIDGET}?start_date={start_date}&end_date={end_date}&time_trunc={TIME_AGGREGATION}{GEO_PARAMS}"

def fetch_data():
    response = requests.get(api_url, headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        for record in data["data"]["attributes"]["values"]:
            es.index(index="asturias_api_data", document=record)
        print("API data uploaded successfully.")
    else:
        print(f"Error: {response.status_code}, {response.json()}")

# Fetch and upload data to Elasticsearch
fetch_data()