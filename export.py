import os
import pandas as pd
from datetime import datetime
import pytz
import csv

def export_action(file_paths):
    print("Exporting data...")
    
    # Match file names to specific data objects
    forsäljning_data = None
    betalsätt_data = None
    följesedlar_data = None

    for file_path in file_paths:
        file_name = os.path.basename(file_path)  # Extract only the file name
        if "Försäljning" in file_name:
            forsäljning_data = pd.read_csv(file_path, sep=';')
        elif "Betalsätt" in file_name:
            betalsätt_data = pd.read_csv(file_path, sep=';')
        elif "Följesedlar" in file_name:
            följesedlar_data = pd.read_csv(file_path, sep=';')

    if forsäljning_data is None or betalsätt_data is None or följesedlar_data is None:
        raise ValueError("One or more required files are missing from the file paths.")

    target_folder = "C:/Users/holme/OneDrive/Skrivbord/Install-Testing-System-Service/Exported_files"

    file_list = create_resulting_files(forsäljning_data, target_folder)

    data_00(file_list)
    
    # Perform additional functionality
    data_01_02(följesedlar_data, file_list)

    data_99(file_list)

    print(f"All files saved to folder: {target_folder}")

    # Add further export functionality here

def create_resulting_files(forsäljning_data, target_folder):
    created_files = []

    # Ensure the target folder exists
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # Get the current timestamp in Stockholm timezone
    stockholm_tz = pytz.timezone("Europe/Stockholm")
    timestamp = datetime.now(stockholm_tz).strftime("%H-%M-%S")

    # Extract unique values from the "Serie" column
    unique_series = forsäljning_data["Serie"].unique()

    for serie in unique_series:
        # Create an empty file for this series in the target folder
        file_name = f"{serie}_{timestamp}.csv"
        file_path = os.path.join(target_folder, file_name)

        # Write an empty file with no data, only an optional placeholder header
        with open(file_path, 'w') as f:
            pass  # Creates an empty file

        created_files.append(file_path)
        print(f"Created empty file: {file_path}")

    return created_files

# Map "Serie" values to file names (ignoring the first two characters)
def map_serie_to_file_name(serie_value):
    return f"T0{serie_value[2:]}"

def data_01_02(följesedlar_data, file_list):

    file_data = {}

    for _, row in följesedlar_data.iterrows():

        serie = row["Serie"]

        #01 Mapped values
        shop_id = row["Id. Shop"]  # Finns både shop_id och id_shop
        cash_register_id = row["Id. Shop"]  # Vet inte vilken denna är
        customer_id = row["Customer Id."]
        date = row["Date"]
        reference = row["Reference"]
        receipt_id = row["Reference"]  # Vet inte vilken denna är
        seller_id = row["Employee"]  # Är detta rätt?

        #02 Mapped values
        article = row["Product"]
        article_id = row["Product"]
        quantity = row["Qty.1"]  # Finns Qty.1 och Qty.
        brutto = row["Gross Amount"]
        netto = row["Net Amount"]
        moms = row["Net Amount"]  # Vet inte vad denna är
        discount = row["Net Amount"]  # Är detta "Discount" eller "Discount Amount"?


        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(f"Warning: Target file {target_file_partial} not found in file_list. Skipping row.")
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        # Create the custom mapping for all rows
        mapped_row_01 = [
            "01",
            shop_id,
            cash_register_id,
            customer_id,
            date,
            reference,
            receipt_id,
            seller_id
        ]

        mapped_row_02 = [
            "02",
            article,
            article_id,
            quantity,
            brutto,
            netto,
            moms,
            discount
        ]

        file_data[matching_file].append(mapped_row_01)
        file_data[matching_file].append(mapped_row_02)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, 'a') as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(','.join(quoted_row) + '\n')
        print(f"Appended rows to file: {target_file}")

def data_03_04(betalsätt_data, file_list):

    file_data = {}

    for _, row in betalsätt_data.iterrows():

        #Mapped values
        serie = row["Serie"]
        number = row["Number"]
        line_number = row["Line Number"]
        store = row["Store"]


        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(f"Warning: Target file {target_file_partial} not found in file_list. Skipping row.")
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        # Create the custom mapping for all rows
        mapped_row_03 = [
            "03",
            serie,
            number,
        ]

        mapped_row_04 = [
            "04",
            line_number,
            store,
        ]

        file_data[matching_file].append(mapped_row_03)
        file_data[matching_file].append(mapped_row_04)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, 'a') as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(','.join(quoted_row) + '\n')
        print(f"Appended rows to file: {target_file}")

def data_05_12(försäljning_data, file_list):

    file_data = {}

    for _, row in försäljning_data.iterrows():

        #Mapped values
        serie = row["Serie"]
        number = row["Number"]
        line_number = row["Line Number"]
        store = row["Store"]


        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(f"Warning: Target file {target_file_partial} not found in file_list. Skipping row.")
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        # Create the custom mapping for all rows
        mapped_row_05 = [
            "03",
            serie,
            number,
        ]

        mapped_row_06 = [
            "03",
            serie,
            number,
        ]

        mapped_row_07 = [
            "03",
            serie,
            number,
        ]

        mapped_row_08 = [
            "03",
            serie,
            number,
        ]

        mapped_row_09 = [
            "03",
            serie,
            number,
        ]

        mapped_row_10 = [
            "03",
            serie,
            number,
        ]

        mapped_row_11 = [
            "03",
            serie,
            number,
        ]

        mapped_row_12 = [
            "04",
            line_number,
            store,
        ]

        file_data[matching_file].append(mapped_row_05)
        file_data[matching_file].append(mapped_row_06)
        file_data[matching_file].append(mapped_row_07)
        file_data[matching_file].append(mapped_row_08)
        file_data[matching_file].append(mapped_row_09)
        file_data[matching_file].append(mapped_row_10)
        file_data[matching_file].append(mapped_row_11)
        file_data[matching_file].append(mapped_row_12)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, 'a') as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(','.join(quoted_row) + '\n')
        print(f"Appended rows to file: {target_file}")

def data_00(file_list):
    header_row = ["00", "20120720_001", "1.3.15"]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, 'a') as f:
            quoted_row = [f'"{value}"' for value in header_row]
            f.write(','.join(quoted_row) + '\n')
        print(f"Added header to file: {target_file}")

def data_99(file_list):
    footer_row = ["99"]

    for target_file in file_list:
        # Append the footer row to each file
        with open(target_file, 'a') as f:
            quoted_row = [f'"{value}"' for value in footer_row]
            f.write(','.join(quoted_row) + '\n')
        print(f"Added footer to file: {target_file}")


