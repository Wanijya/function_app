import azure.functions as func
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="wanijya_trigger")
def wanijya_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    connection_string = os.getenv("AzureWebJobsStorage")
    if not connection_string:
        logging.error("Azure storage connection string not found in environment variables.")
        return func.HttpResponse("Error: Connection string not found.", status_code=500)

    container_name = "wanijya-container"
    source_blob_name1 = "inputData1.json" 
    source_blob_name2 = "inputData2.json"
    destination_blob_name = "mergedData.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:

        blob_client1 = container_client.get_blob_client(source_blob_name1)
        if not blob_client1.exists():
            logging.error(f"The blob '{source_blob_name1}' does not exist in the container '{container_name}'.")
            return func.HttpResponse("Error: Source blob 1 not found.", status_code=404)

        downloaded_blob1 = blob_client1.download_blob().readall()
        json_data1 = json.loads(downloaded_blob1)
        df1 = pd.DataFrame(json_data1)

        blob_client2 = container_client.get_blob_client(source_blob_name2)
        if not blob_client2.exists():
            logging.error(f"The blob '{source_blob_name2}' does not exist in the container '{container_name}'.")
            return func.HttpResponse("Error: Source blob 2 not found.", status_code=404)

        downloaded_blob2 = blob_client2.download_blob().readall()
        json_data2 = json.loads(downloaded_blob2)
        df2 = pd.DataFrame(json_data2)

        merged_df = pd.merge(df1, df2, on="id", how="inner")

        csv_data = merged_df.to_csv(index=False).encode("utf-8")

        new_blob_client = container_client.get_blob_client(destination_blob_name)
        new_blob_client.upload_blob(csv_data, overwrite=True)

        logging.info(f"File '{destination_blob_name}' uploaded to container '{container_name}'  with merged data.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse("An error occurred while processing the files.", status_code=500)

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")
            
    if name:
        return func.HttpResponse(f"Hello, {name}. The JSON files were merged and saved successfully .")
    else:
        return func.HttpResponse("The JSON files were merged and saved successfully . Pass a name in the query string or in the request body for a personalized response.", status_code=200)