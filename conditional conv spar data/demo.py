import os
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import csv
import xml.etree.ElementTree as ET

#================================================================
def is_xml(ip_content):
            try:
                ET.fromstring(ip_content)
                return True
            except ET.ParseError:
                return False
            except Exception as e:
                print(f"Error while checking XML format: {e}")
                return False
            
def is_csv(ip_content):
            try:
                csv.reader(ip_content)
                return True
            except csv.Error:
                return False
            except Exception as e:
                print(f"Error while checking CSV format: {e}")
                return False
#================================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get parameters from query string
    ip_blob_name = req.params.get('ip_blob_name')
    op_blob_name = req.params.get('op_blob_name')
    source_container_name = req.params.get('source_container_name')
    dest_container_name = req.params.get('dest_container_name')

    if not ip_blob_name or not op_blob_name or not source_container_name or not dest_container_name:
        return func.HttpResponse(
             "Please provide 'ip_blob_name', 'source_container_name', 'dest_container_name' and 'op_blob_name' parameters.",
             status_code=400
        )

    try:
        ip_connection_string = 'DefaultEndpointsProtocol=https;AccountName=functiondata537;AccountKey=0TVhXnuC6IoUOixkfHZebbJ2pJ2DPRvM2UT8C54S+lTQ8bthz61ALFcOdkBmoCL8O0hXsY13I5dz+AStApBR/w==;EndpointSuffix=core.windows.net'
        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string (ip_connection_string)
        
        blob_container_client = blob_service_client.get_container_client (source_container_name)
        blob_client = blob_container_client.get_blob_client (ip_blob_name)
        
        # Download the input file content
        ip_content = blob_client.download_blob().readall()

        #================================================================
        if is_xml(ip_content):
            df = pd.read_xml(ip_content)
            # Convert XML to CSV
            csv_content = df.to_csv(index=False)
            # Upload CSV to Blob Storage

            op_connection_string = 'DefaultEndpointsProtocol=https;AccountName=functiondata537;AccountKey=0TVhXnuC6IoUOixkfHZebbJ2pJ2DPRvM2UT8C54S+lTQ8bthz61ALFcOdkBmoCL8O0hXsY13I5dz+AStApBR/w==;EndpointSuffix=core.windows.net'
            op_blob_service_client = BlobServiceClient.from_connection_string (op_connection_string)

            csv_container_client = op_blob_service_client.get_container_client (dest_container_name)
            csv_blob_client = csv_container_client.get_blob_client (op_blob_name)

            csv_blob_client.upload_blob(csv_content, overwrite=True)

            return func.HttpResponse(f"XML to CSV conversion successful. CSV file '{op_blob_name}' uploaded.")
        
        elif is_csv(ip_content):
            df = pd.read_csv(ip_content)
            # Convert the DataFrame to XML
            xml_content = df.to_xml(index=False)

            # Upload CSV to Blob Storage

            op_connection_string = 'DefaultEndpointsProtocol=https;AccountName=functiondata537;AccountKey=0TVhXnuC6IoUOixkfHZebbJ2pJ2DPRvM2UT8C54S+lTQ8bthz61ALFcOdkBmoCL8O0hXsY13I5dz+AStApBR/w==;EndpointSuffix=core.windows.net'
            op_blob_service_client = BlobServiceClient.from_connection_string (op_connection_string)

            xml_container_client = op_blob_service_client.get_container_client (dest_container_name)
            xml_blob_client = xml_container_client.get_blob_client (op_blob_name)

            xml_blob_client.upload_blob(xml_content, overwrite=True)

            return func.HttpResponse(f"XML to CSV conversion successful. CSV file '{op_blob_name}' uploaded.")
             

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
             
