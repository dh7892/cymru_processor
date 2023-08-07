import os
import xml.etree.ElementTree as ET
import csv
from tqdm import tqdm

# Function to extract data from <reputation> element
def extract_reputation_data(reputation):
    data = {}
    data["stamp"] = reputation.findtext("stamp")
    data["addr"] = reputation.findtext("addr")
    data["notes"] = reputation.findtext("notes")
    data["cc"] = reputation.findtext("cc")
    data["reputation_key"] = reputation.findtext("reputation_key")
    data["proto"] = reputation.findtext("proto")
    data["family"] = reputation.findtext("family")
    data["asn"] = reputation.findtext("asn")
    data["category"] = reputation.findtext("category")
    data["reputation_score"] = reputation.findtext("reputation_score")
    data["port"] = reputation.findtext("port")
    
    # Parse the reputation_key field
    reputation_key_element = reputation.find("reputation_key")
    reputation_key = reputation_key_element.text if reputation_key_element is not None else ""
    data.update(parse_reputation_key(reputation_key))

    return data

# Function to parse reputation_key field and split it into separate fields
def parse_reputation_key(reputation_key):
    # Ensure reputation_key is a string (default to empty string if None)
    reputation_key = reputation_key or ""
    fields = {
        "A": "field_a",
        "B": "field_b",
        "C": "field_c",
        "D": "field_d",
        "E": "field_e",
        "F": "field_f",
        "G": "field_g",
        "H": "field_h",
        "I": "field_i",
        "J": "field_j",
        "K": "field_k",
    }

    parsed_data = {}
    current_field = None
    value_str = ""

    for char in reputation_key:
        if char in fields:
            if current_field is not None:
                parsed_data[fields[current_field]] = int(value_str)
                value_str = ""

            current_field = char
        else:
            value_str += char

    if current_field is not None:
        parsed_data[fields[current_field]] = int(value_str)

    return parsed_data

# Function to estimate the average element size using the first 100 elements
def estimate_avg_element_size(xml_file_path, num_elements=100):
    total_element_size = 0
    with open(xml_file_path, "r") as xmlfile:
        context = ET.iterparse(xmlfile, events=("start",))
        for _, elem_tuple in zip(range(num_elements), context):
            elem = elem_tuple[1]  # Access the element from the tuple
            total_element_size += len(ET.tostring(elem, encoding="unicode"))
            elem.clear()
    avg_element_size = total_element_size // num_elements
    return avg_element_size

# Function to get an estimate of the total number of <reputation> elements
def estimate_total_elements(xml_file_path):
    file_size = os.path.getsize(xml_file_path)
    # Assuming an average element size, estimate the total elements
    # Estimate the average element size using the first 100 elements
    avg_element_size = estimate_avg_element_size(xml_file_path)
    return file_size // avg_element_size

# Function to process XML file incrementally and write to CSV
def xml_to_csv(xml_file_path, output_folder, chunk_size=1000, max_rows_per_file=5000000):
    # Get an estimate of the total number of <reputation> elements
    total_elements = estimate_total_elements(xml_file_path)
    print(f"Estimated total number of <reputation> elements: {total_elements}")

    # Initialize a counter for the rows written to the current CSV file
    rows_written = 0

    # Initialize the XML parser
    context = ET.iterparse(xml_file_path, events=("start",))

    # Initialize the output CSV file number
    file_number = 1

    current_csvfile = open(os.path.join(output_folder, f"repfeed_{file_number}.csv"), "w", newline="")
    fieldnames = [
        "stamp", "addr", "notes", "cc", "reputation_key",
        "proto", "family", "asn", "category", "reputation_score", "port",
        "field_a", "field_b", "field_c", "field_d", "field_e",
        "field_f", "field_g", "field_h", "field_i", "field_j", "field_k"
    ]
    writer = csv.DictWriter(current_csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Initialize tqdm progress bar with total_elements
    progress_bar = tqdm(total=total_elements, unit="reputation", desc="Processing")

    # Process the XML file in chunks
    for event, elem in context:
        if elem.tag == "reputation":
            data = extract_reputation_data(elem)
            writer.writerow(data)
            rows_written += 1

            # If the maximum number of rows per file is reached, close the current file
            # and start writing to a new file with the incremented file number
            if rows_written == max_rows_per_file:
                current_csvfile.close()
                rows_written = 0
                file_number += 1
                current_csvfile = open(os.path.join(output_folder, f"repfeed_{file_number}.csv"), "w", newline="")
                writer = csv.DictWriter(current_csvfile, fieldnames=fieldnames)
                writer.writeheader()

            # Update the progress bar
            progress_bar.update()

            # Clear the processed element to free up memory
            elem.clear()

    # Close the last CSV file after finishing processing
    current_csvfile.close()

    # Close the progress bar after finishing processing
    progress_bar.close()

if __name__ == "__main__":
    xml_file_path = "repfeed_202308010700.xml"
    output_folder = "repfeed_output"

    xml_to_csv(xml_file_path, output_folder)