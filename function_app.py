import azure.functions as func 
import logging
import pandas as pd  
from azure.storage.blob import BlobServiceClient
import os
import io
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="wanijya_trigger")
def wanijya_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    connection_string = os.getenv("AzureWebJobsStorage")
    if not connection_string:
        logging.error("Azure storage connection string not found in environment variables.")
        return func.HttpResponse("Error: Connection string not found.", status_code=500)

    container_name = "wanijya-container"
    source_blob_name = "Amazon-Wanijya.json"
    destination_blob_name = "OutputFile.csv"

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Ensure container exists
        try:
            container_client.get_container_properties()
        except Exception:
            logging.info(f"Container '{container_name}' does not exist. Creating it...")
            container_client.create_container()
            logging.info(f"Container '{container_name}' created successfully.")

        # Ensure blob exists before download
        blob_client = container_client.get_blob_client(source_blob_name)
        if not blob_client.exists():
            logging.error(f"Blob '{source_blob_name}' does not exist in container '{container_name}'.")
            return func.HttpResponse(f"Error: Blob '{source_blob_name}' does not exist.", status_code=404)

        downloaded_blob = blob_client.download_blob().readall()
        json_data = json.loads(downloaded_blob)
        json_df = pd.DataFrame(json_data)

        # Append new data
        new_data = {
            "id": ["4"],
            "name": ["Product B"],
            "price": [19.99],
            "rating": [4.5],
            "category": ["Books"],
            "description": ["An inspiring novel by a bestselling author."]
        }
        new_data_df = pd.DataFrame(new_data)
        appended_data = pd.concat([json_df, new_data_df], ignore_index=True)

        # Convert to CSV and upload
        csv_data = appended_data.to_csv(index=False).encode('utf-8')
        new_blob_client = container_client.get_blob_client(destination_blob_name)
        new_blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f"File '{destination_blob_name}' uploaded to container '{container_name}' as a CSV format with appended data.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse("An error occurred while processing the file.", status_code=500)

    # Check for name in request
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    # Return response with optional name greeting
    if name:
        return func.HttpResponse(f"Hello, {name}. The JSON file was converted, appended, and uploaded successfully as CSV.")
    else:
        return func.HttpResponse("The JSON file was converted, appended, and uploaded successfully as CSV. Pass a name in the query string or in the request body for a personalized response.", status_code=200)
