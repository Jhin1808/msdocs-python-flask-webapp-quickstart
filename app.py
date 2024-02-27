import os
import json
import sys
import azure.cosmos.documents as documents
from azure.cosmos import CosmosClient
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import requests

from azure.storage.blob import BlobServiceClient

import pymongo
from dotenv import load_dotenv


from flask import (Flask, flash, redirect, render_template, request,
                   send_from_directory, url_for)


app = Flask(__name__)

secret_key = os.urandom(24)
app.secret_key = secret_key


@app.route('/')
def index():
    print('Request for index page received')
    return render_template('index.html')


@app.route('/data_management')
def data_management():
    print('Request for data management page received')
    return render_template('data_management.html')


def parse_data_line(line):
    parts = line.split()  # Split into words initially
    name = parts[0]  # First word is the name

    attributes = {}
    for part in parts[1:]:  # Process remaining words
        if '=' in part:
            key, value = part.split('=')
            attributes[key] = value
        else:  # Could be additional names
            name += " " + part

    return name, attributes


def parse_data(data):  # Modified to work on the entire file content
    parsed_data = []
    for line in data.splitlines():
        name, attributes = parse_data_line(line)
        # Structure for Cosmos DB
        parsed_data.append({'name': name, 'attributes': attributes})
    return parsed_data


@app.route('/load_data', methods=['POST'])
def load_data():
   
    try:
        response = requests.get(
            'https://css490.blob.core.windows.net/lab4/input.txt')
        if response.status_code == 200:
            flash('Data Loaded Successfully')
            data = response.text
            print(data)
            blob_service_client = BlobServiceClient.from_connection_string(
                "DefaultEndpointsProtocol=https;AccountName=dbprog4;AccountKey=90uEgUBq8AJj+JS+8/T6QHB3CmN/opCy5TbzRpDFWChu7syAHp7bBrCvGuMo1wHKDTiEYMcuUP1Y+AStTYjQ3g==;EndpointSuffix=core.windows.net")
            container_name = "blobdb"
            blob_name = "input.txt"
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob_name)
            blob_client.upload_blob(data, overwrite=True)
            # 3. Parse the Data
            parsed_data = parse_data(data)

            # 4. Connect to Cosmos DB
            client = CosmosClient(
                "https://luan.documents.azure.com:443/", credential="wJV9J2GhQXoC0RPeBy5jECLUzeCPJLMmcKuYPPfQFT9OcFxH8Ut2ynuE4o1eM8LxZOMGLnYrzmYAACDbBf18Vg==")

            database = client.get_database_client('WebDB')
            container = database.get_container_client("Name")

            for item in parsed_data:
                # Use 'myid' if available else the name
                item['id'] = item.get('myid', item['name'])
                container.upsert_item(item)

        else:
            print(f"Failed to load data, status code: {response.status_code}")
    except Exception as e:
        print(f"Error loading data: {e}")
    return redirect(url_for('data_management'))


if __name__ == '__main__':
    app.run()
