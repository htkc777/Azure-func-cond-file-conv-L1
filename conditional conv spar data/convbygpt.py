import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
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
    root = ET.fromstring(xml_content)
    csv_rows = []
    for item in root.findall("Item"):
        row = {}
        for field in item:
            row[field.tag] = field.text
        csv_rows.append(row)
    return csv_rows

def main(req: func.HttpRequest) -> str:
    try:
        storage_connection_string = "<your_connection_string>"
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_name = "<your_container_name>"
        blob_name = "<your_blob_name>"

        # Fetch the input file from Azure Blob Storage
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob()
        file_content = blob_data.readall()

        # Check if the file is CSV or XML
        is_csv = False
        if blob_name.endswith(".csv"):
            is_csv = True

        if is_csv:
            # Convert CSV to XML
            csv_content = csv.DictReader(file_content.decode("utf-8").splitlines())
            xml_content = convert_csv_to_xml(csv_content)
            new_blob_name = blob_name.replace(".csv", ".xml")
        else:
            # Convert XML to CSV
            xml_content = file_content.decode("utf-8")
            csv_content = convert_xml_to_csv(xml_content)
            new_blob_name = blob_name.replace(".xml", ".csv")

        # Upload the converted file to Azure Blob Storage
        new_blob_client = container_client.get_blob_client(new_blob_name)
        new_blob_client.upload_blob(xml_content if is_csv else ",".join(csv_content))
        
        return f"File converted and uploaded as {new_blob_name}"
    except Exception as e:
        return str(e)

