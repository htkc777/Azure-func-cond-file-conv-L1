import os
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import csv
import xml.etree.ElementTree as ET
import tempfile

def convert_csv_to_xml(csv_content):
    xml_root = ET.Element("Data")
    for row in csv_content:
        item = ET.SubElement(xml_root, "Item")
        for column in row:
            field = ET.SubElement(item, column)
            field.text = row[column]
    return ET.tostring(xml_root, encoding="unicode")

def convert_xml_to_csv(xml_content):
    df = pd.read_xml(xml_content)
    # Convert XML to CSV
    csv_content = df.to_csv(index=False)
    # Upload CSV to Blob Storage
    return csv_content

def main(req: func.HttpRequest) -> str:

    # Get parameters from query string
    ip_blob_name = req.params.get('ip_blob_name')
    op_blob_name = req.params.get('op_blob_name')
    source_container_name = req.params.get('source_container_name')
    dest_container_name = req.params.get('dest_container_name')

    try:
        ip_connection_string = "DefaultEndpointsProtocol=https;AccountName=xyz1500;AccountKey=GdrlFjxAD8NHNnnTCijmngfaO1QIfBOiozXdEn5JeOE+eNp6XSJq2ka6eVwDcpnu3EydYU+BDQmC+AStsjUU3Q==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(ip_connection_string)

        # Fetch the input file from Azure Blob Storage
        blob_container_client = blob_service_client.get_container_client (source_container_name)
        blob_client = blob_container_client.get_blob_client (ip_blob_name)

        # Download the input file content
        ip_content = blob_client.download_blob().readall()

        # Check if the file is CSV or XML
        is_csv = False
        if ip_blob_name.endswith(".csv"):
            is_csv = True

        if is_csv:
            # Convert CSV to XML
            csv_content = csv.DictReader(ip_content.decode("utf-8").splitlines())
            xml_content = convert_csv_to_xml(csv_content)
        else:
            # Convert XML to CSV
            xml_content = ip_content.decode("utf-8")
            csv_content = convert_xml_to_csv(xml_content)

        # Upload the converted file to Azure Blob Storage
        # new_blob_client = container_client.get_blob_client(new_blob_name)

        op_connection_string = 'DefaultEndpointsProtocol=https;AccountName=xyz1500;AccountKey=GdrlFjxAD8NHNnnTCijmngfaO1QIfBOiozXdEn5JeOE+eNp6XSJq2ka6eVwDcpnu3EydYU+BDQmC+AStsjUU3Q==;EndpointSuffix=core.windows.net'
        op_blob_service_client = BlobServiceClient.from_connection_string (op_connection_string)

        op_container_client = op_blob_service_client.get_container_client (dest_container_name)
        op_blob_client = op_container_client.get_blob_client (op_blob_name)

        op_blob_client.upload_blob(xml_content if is_csv else ",".join(csv_content))
        
        return f"File converted and uploaded as {op_blob_name}"
    except Exception as e:
        return str(e)