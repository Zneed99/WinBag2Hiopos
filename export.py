import os
import pandas as pd
from datetime import datetime
import pytz
import csv
import time


def export_action(file_paths):
    # Match file names to specific data objects
    forsäljning_data = None
    betalsätt_data = None
    följesedlar_data = None
    time.sleep(1)

    for file_path in file_paths:
        file_name = os.path.basename(file_path)  # Extract only the file name
        if "Försäljning" in file_name:
            forsäljning_data = pd.read_csv(file_path, sep=";")
        elif "Betalsätt" in file_name:
            betalsätt_data = pd.read_csv(file_path, sep=";")
        elif "Följesedlar" in file_name:
            följesedlar_data = pd.read_csv(file_path, sep=";")

    if forsäljning_data is None or betalsätt_data is None or följesedlar_data is None:
        raise ValueError("One or more required files are missing from the file paths.")

    target_folder = "C:/Users/FelixHolmesten/OneDrive - LexEnergy/Skrivbordet/InstallSystemService/Exported_Files"

    file_list = create_resulting_files(forsäljning_data, target_folder)

    data_00(file_list)

    # Perform additional functionality
    data_01_02(följesedlar_data, file_list)
    data_03_04(betalsätt_data, file_list)

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

    print(f"Unique series: {unique_series}")

    for serie in unique_series:
        # Create an empty file for this series in the target folder
        file_name = f"{serie}_{timestamp}.csv"
        file_path = os.path.join(target_folder, file_name)

        # Write an empty file with no data, only an optional placeholder header
        with open(file_path, "w") as f:
            pass  # Creates an empty file

        created_files.append(file_path)

    return created_files


# Map "Serie" values to file names (ignoring the first two characters)
def map_serie_to_file_name(serie_value):
    return f"T0{serie_value[2:]}"


def data_01_02(följesedlar_data, file_list):

    file_data = {}
    current_number = None

    for _, row in följesedlar_data.iterrows():

        serie = row["Serie"]
        number = row["Nummer"]

        # 01 Mapped values
        shop_id = row["Butikskod"]
        cash_register_id = row["KassaId"]
        customer_id = row["Kundkod"]
        date = row["Datum"]
        reference = row["Referens"]
        receipt_id = row["Referens"]  # Vet inte vilken denna är
        seller_id = row["Anställd"]

        # 02 Mapped values
        article = row["Artikel"]
        article_id = row["Artikel"]  # Vet inte vilken denna är
        quantity = row["Ant."]
        brutto = row["Bas"]  # Är detta rätt?
        netto = row["Netto"]
        moms = row["Total moms"]
        discount = row["Total moms"]  # Vet inte vilken denna är

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(
                f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
            )
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        # Create the custom mapping for all rows
        # Only add a new "01" row if the "Nummer" changes
        if current_number != number:
            mapped_row_01 = [
                "01",
                shop_id,
                cash_register_id,
                customer_id,
                date,
                reference,
                receipt_id,
                seller_id,
            ]
            file_data[matching_file].append(mapped_row_01)
            current_number = number  # Update the current "Nummer"

        mapped_row_02 = [
            "02",
            article,
            article_id,
            quantity,
            brutto,
            netto,
            moms,
            discount,
        ]

        file_data[matching_file].append(mapped_row_02)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")
        # print(f"Appended rows to file: {target_file}")


def data_03_04(betalsätt_data, file_list):

    file_data = {}
    current_number = None
    last_number = None  # Track the last Radnummer for conditions
    betalmedel_sums = {}  # Dictionary to store sums for each betalmedel
    processed_betalmedel_for_number = {}

    for _, row in betalsätt_data.iterrows():

        serie = row["Serie"]
        number = row["Nummer"]
        dokumenttyp = row["Dokumenttyp"]

        # 03 Mapped values
        butiks_nr = row["Dok.Id"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

        # 04 Mapped values
        konto = row["Dok.Id"]
        betalmedel = row["Betalmedel"]
        debetbelopp = float(str(row["Totalt belopp"]).replace(",", "."))
        kreditbelopp = float(str(row["Belopp"]).replace(",", "."))

        # Initialize tracking for the current `number` if not done yet
        if number not in processed_betalmedel_for_number:
            processed_betalmedel_for_number[number] = set()

        # Handle sums based on the row's document type and Radnummer change
        if betalmedel not in processed_betalmedel_for_number[number]:
            # Only sum when the betalmedel is unique for this number
            if dokumenttyp == "Sale receipt":
                if betalmedel not in betalmedel_sums:
                    betalmedel_sums[betalmedel] = {"debet": 0, "kredit": 0}
                betalmedel_sums[betalmedel]["debet"] += debetbelopp
                if betalmedel == "KORT":
                    print(f"Sum for KORT: {betalmedel_sums[betalmedel]['debet']}")
            elif dokumenttyp == "Sale receipt return":
                if betalmedel not in betalmedel_sums:
                    betalmedel_sums[betalmedel] = {"debet": 0, "kredit": 0}
                betalmedel_sums[betalmedel]["kredit"] += abs(kreditbelopp)

            # Mark this betalmedel as processed for the current number
            processed_betalmedel_for_number[number].add(betalmedel)

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(
                f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
            )
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

    for betalmedel, sums in betalmedel_sums.items():
        debetbelopp = sums["debet"]
        kreditbelopp = sums["kredit"]
        print(
            f"betalmedel: {betalmedel}, debetbelopp: {debetbelopp}, kreditbelopp: {kreditbelopp}"
        )

    # Create the custom mapping for all rows
    # Only add a new "03" row if the "Nummer" changes
    if current_number == None:
        mapped_row_03 = ["03", butiks_nr, kassa_nr, datum]
        file_data[matching_file].append(mapped_row_03)
        current_number = number  # Update the current "Nummer"

    # # Add the "04" rows based on stored sums
    for betalmedel, sums in betalmedel_sums.items():
        debetbelopp = sums["debet"]
        kreditbelopp = sums["kredit"]
        mapped_row_04 = ["04", betalmedel, str(debetbelopp), str(kreditbelopp)]
        file_data[matching_file].append(mapped_row_04)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")
        print(f"Appended rows to file: {target_file}")


def data_05_12(försäljning_data, file_list):

    file_data = {}

    for _, row in försäljning_data.iterrows():

        # Mapped values
        serie = row["Serie"]
        number = row["Number"]
        line_number = row["Line Number"]
        store = row["Store"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(
                f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
            )
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
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")
        print(f"Appended rows to file: {target_file}")


def data_00(file_list):
    header_row = ["00", "20120720_001", "1.3.15"]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in header_row]
            f.write(",".join(quoted_row) + "\n")
        print(f"Added header to file: {target_file}")


def data_99(file_list):
    footer_row = ["99"]

    for target_file in file_list:
        # Append the footer row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in footer_row]
            f.write(",".join(quoted_row) + "\n")
        print(f"Added footer to file: {target_file}")
