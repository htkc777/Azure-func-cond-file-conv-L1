import csv
import xml.etree.ElementTree as ET

def is_xml(file_path):
    try:
        with open(file_path, 'r') as file:
            # Attempt to parse the file as XML
            ET.fromstring(file.read())
            return True
    except ET.ParseError:
        return False
    except Exception as e:
        print(f"Error while checking XML format: {e}")
        return False

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

def check_file_format(file_path):
    if is_xml(file_path):
        print(f"{file_path} is an XML file.")
    elif is_csv(file_path):
        print(f"{file_path} is a CSV file.")
    else:
        print(f"{file_path} is neither XML nor CSV.")

if __name__ == '__main__':
    file_path = input("Enter the file path: ")
    check_file_format(file_path)
