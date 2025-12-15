import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, GridSearchCV
from xgboost import XGBRegressor
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from elasticsearch import Elasticsearch
from sklearn.inspection import partial_dependence
import re
from dotenv import load_dotenv
import os
# Load environment variables
load_dotenv()

# Initialize Elasticsearch client
def create_es_client():
    es = Elasticsearch(
        hosts=[{
            'host': 'localhost',  # Change 'localhost' if connecting to a Docker container
            'port': 9200,
            'scheme': 'http'  # Explicitly specify the scheme
        }],
        basic_auth=('elastic', os.getenv("ELASTIC_PASSWORD"))
    )
    if es.ping():
        print("Connected to Elasticsearch")
    else:
        raise ConnectionError("Failed to connect to Elasticsearch")
    return es

# Fetch data from a single Elasticsearch index
def fetch_data_from_index(es, index_name, size=1000):
    # Use a scroll to retrieve all documents
    query = {"query": {"match_all": {}}}
    response = es.search(index=index_name, body=query, scroll='2m', size=size)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

    while len(response['hits']['hits']):
        response = es.scroll(scroll_id=scroll_id, scroll='2m')
        hits.extend(response['hits']['hits'])

    records = [hit["_source"] for hit in hits]
    return pd.DataFrame(records)

# Group indices and fetch combined data
def fetch_grouped_data(es, group_pattern, size=1000):
    all_indices = es.cat.indices(format="json")
    matching_indices = [
        index['index'] for index in all_indices if re.match(group_pattern, index['index'])
    ]

    combined_data = pd.DataFrame()
    for index_name in matching_indices:
        print(f"Fetching data from index: {index_name}")
        df = fetch_data_from_index(es, index_name, size)
        combined_data = pd.concat([combined_data, df], ignore_index=True)
    
    return combined_data

def merge():
    data_dir = "temp"

    dataframes["tarifa"] = tarifa
    dataframes["autoconsumo"] = autoconsumo
    dataframes["paro"] = paro
    dataframes["generacion"] = generacion
    dataframes["balance"] = balance
    dataframes["inundacion"] = inundacion
    dataframes["lim_geo"] = lim_geo
    dataframes["econ"] = econ

    # Filter for Asturias
    filtered_dataframes = {}
    for name, df in dataframes.items():
        if 'province' in df.columns:
            filtered_dataframes[name] = df[df['province'].str.contains('Asturias', case=False, na=False)]
        else:
            filtered_dataframes[name] = df  # Keep dataset as is if 'province' is not a column

    # Merge datasets
    merged_data = filtered_dataframes['autoconsumo']
    if 'province' in merged_data.columns and 'province' in filtered_dataframes['paro'].columns:
        merged_data = pd.merge(
            merged_data,
            filtered_dataframes['paro'],
            on=['province'],
            how='left'
        )
    if 'dataDate' in merged_data.columns and 'dataDate' in filtered_dataframes['tarifa'].columns:
        merged_data = pd.merge(
            merged_data,
            filtered_dataframes['tarifa'],
            on=['province', 'dataDate'],
            how='left'
        )
    if 'dataDate' in merged_data.columns and 'dataDate' in filtered_dataframes['generacion'].columns:
        merged_data = pd.merge(
            merged_data,
            filtered_dataframes['generacion'],
            on='dataDate',
            how='left'
        )
    if 'dataDate' in merged_data.columns and 'dataDate' in filtered_dataframes['balance'].columns:
        merged_data = pd.merge(
            merged_data,
            filtered_dataframes['balance'],
            on='dataDate',
            how='left'
        )

    # Feature Engineering
    merged_data['energy_per_contract'] = (
        merged_data['sumEnergy_kWh'] / merged_data['sumContracts_y'].replace(0, 1)
    )
    merged_data['selfConsumption_encoded'] = merged_data['selfConsumption'].astype('category').cat.codes
    merged_data['power_per_contract'] = (
        merged_data['sumPower_kW'] / merged_data['sumContracts_x'].replace(0, 1)
    )
    # Save the merged and processed data
    output_path = os.path.join(data_dir, "merged_asturias_data.csv")
    
    merged_data.to_csv(output_path, index=False)

    print(f"Merged dataset saved at: {output_path}")

    return merged_data

def model():
    # Define the target and features
    target = "energy_per_contract"
    features = data.drop(columns=[target, "dataDate", "province", "municipality"])

    X = features.select_dtypes(include=['number'])  # Select only numeric features
    y = data[target]

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the original model
    model = XGBRegressor(n_estimators=150, learning_rate=0.2, max_depth=7, random_state=42)
    model.fit(X_train, y_train)

    # Feature importance analysis
    importance = model.feature_importances_
    top_features = [feature for feature, imp in zip(X.columns, importance) if imp > 0.01]

    # Simplify dataset
    simplified_X = X[top_features]
    X_train_simplified, X_test_simplified, y_train_simplified, y_test_simplified = train_test_split(
        simplified_X, y, test_size=0.2, random_state=42
    )

    # Retrain the model with simplified features
    simplified_model = XGBRegressor(n_estimators=150, learning_rate=0.2, max_depth=7, random_state=42)
    simplified_model.fit(X_train_simplified, y_train_simplified)

    # Evaluate the simplified model
    y_pred = simplified_model.predict(X_test_simplified)
    simplified_rmse = root_mean_squared_error(y_test_simplified, y_pred)
    simplified_mae = mean_absolute_error(y_test_simplified, y_pred)
    simplified_r2 = r2_score(y_test_simplified, y_pred)

    print(f"Simplified Model Metrics:\nRMSE: {simplified_rmse}\nMAE: {simplified_mae}\nR²: {simplified_r2}")

    # Correlation Heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(simplified_X.corr(), annot=True, fmt=".2f", cmap="coolwarm")
    plt.title("Correlation Heatmap of Top Features")
    plt.show()

    # Partial Dependence Plots
    for feature in top_features:
        plt.figure(figsize=(8, 6))
        pdp_results = partial_dependence(simplified_model, X_test_simplified, features=[feature])
        plt.plot(pdp_results["average"][0], label=feature)
        plt.xlabel(feature)
        plt.ylabel("Partial Dependence")
        plt.title(f"Partial Dependence of {feature} on Target")
        plt.legend()
        plt.show()

    # Scatterplots for Features vs Target
    for feature in top_features:
        plt.figure(figsize=(8, 6))
        sns.scatterplot(x=simplified_X[feature], y=y, alpha=0.5)
        plt.title(f"{feature} vs Target")
        plt.xlabel(feature)
        plt.ylabel(target)
        plt.show()

def save_csvs():
    tarifa.to_csv("temp/tarifa.csv", index='index', encoding='ISO-8859-1')
    autoconsumo.to_csv("temp/autoconsumo.csv", index='index', encoding='ISO-8859-1')
    paro.to_csv("temp/paro.csv", index='index', encoding='ISO-8859-1')
    generacion.to_csv("temp/generacion.csv", index=False, encoding='ISO-8859-1')
    balance.to_csv("temp/balance.csv", index=False, encoding='ISO-8859-1')
    inundacion.to_csv("temp/inundacion.csv", index='index', encoding='ISO-8859-1')
    lim_geo.to_csv("temp/lim_geo.csv", index='index', encoding='ISO-8859-1')
    econ.to_csv("temp/econ.csv", index='index', encoding='ISO-8859-1')

if __name__ == "__main__":
    es_client = create_es_client()

    # Define groups for indices
    group_patterns = {
        "tarifa": "tarifa",
        "autoconsumo": "autoconsumo",
        "paro": "paro",
        "inundacion": "inundacion",
        "lim_geo": "lim_geo",
        "econ": "econ",
        "generacion": r"^generacion_.*$",
        "balance": r"^balance_.*$"
    }

    # Dictionary to hold all combined DataFrames
    dataframes = {}

    # Process CSV-indexed and API-indexed data
    for name, pattern in group_patterns.items():
        data = fetch_grouped_data(es_client, pattern)
        if not data.empty:
            print(f"Columns in {name}:")
            print(data.columns)
            dataframes[name] = data

    for name, df in dataframes.items():
        print(f"{name} df contains {len(df)} rows.")
        
        tarifa = dataframes["tarifa"]
        autoconsumo = dataframes["autoconsumo"]
        paro = dataframes["paro"]
        generacion = dataframes["generacion"]
        balance = dataframes["balance"]
        inundacion = dataframes["inundacion"]
        lim_geo = dataframes["lim_geo"]
        econ = dataframes["econ"]
       
        # Codigo_provincia 33 es Asturias, nos queremos quedar solo con este valor
        paro = paro[paro["codigo_provincia"] == 33]

        # Merge all dataframes
        data = merge()
        model()
        save_csvs()