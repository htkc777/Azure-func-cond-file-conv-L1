import csv
import pandas as pd
import xml.etree.ElementTree as ET

def is_csv(file_path):
    try:
        with open(file_path, 'r') as file:
            # Attempt to read the file as CSV
            csv.reader(file)
            return True
    except csv.Error:
        return False
    except Exception as e:
        print(f"Error while checking CSV format: {e}")
        return False
    
file_path = 'MOCK_DATA.csv'

if is_csv(file_path):
    # Load the CSV file into a pandas DataFrame
    csv_file = 'MOCK_DATA.csv'  # Replace with your CSV file name
    df = pd.read_csv(csv_file)

    # Convert the DataFrame to XML
    xml_data = df.to_xml(index=False)

    # Save the XML to a file
    xml_file = "output.xml"  # Replace with the desired XML file name
    with open(xml_file, 'w') as f:
        f.write(xml_data)

    print(f"CSV file '{csv_file}' has been converted to XML as '{xml_file}'.")
