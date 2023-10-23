import pandas as pd

# Load the CSV file into a pandas DataFrame
csv_file = "your_data.csv"  # Replace with your CSV file name
df = pd.read_csv(csv_file)

# Convert the DataFrame to XML
xml_data = df.to_xml(index=False)

# Save the XML to a file
xml_file = "output.xml"  # Replace with the desired XML file name
with open(xml_file, 'w') as f:
    f.write(xml_data)

print(f"CSV file '{csv_file}' has been converted to XML as '{xml_file}'.")
