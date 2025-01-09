import os
import pandas as pd
from datetime import datetime
import pytz
import time


def export_action(file_paths):
    # Match file names to specific data objects
    forsäljning_data = None
    forsäljning_data10 = None
    betalsätt_data = None
    följesedlar_data = None
    time.sleep(1)

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        if "Försäljning" in file_name:
            forsäljning_data = pd.read_csv(file_path, sep=";", dtype={"Referens": str, "Netto": str})
        elif "Betalsätt" in file_name:
            betalsätt_data = pd.read_csv(file_path, sep=";")
        elif "Följesedlar" in file_name:
            följesedlar_data = pd.read_csv(file_path, sep=";")
        elif "Moms" in file_name:
            moms_data = pd.read_csv(file_path, sep=";")

    if (
        forsäljning_data is None
        or betalsätt_data is None
        or följesedlar_data is None
        or moms_data is None
    ):
        raise ValueError("One or more required files are missing from the file paths.")

    file_path = file_paths[0]
    base_dir = os.path.dirname(file_path)
    export_folder = os.path.join(base_dir, "Exported Files")

    file_list = create_resulting_files(forsäljning_data, export_folder)

    data_00(file_list)

    # Perform additional functionality
    data_01_02(följesedlar_data, file_list)
    data_03(forsäljning_data, file_list)
    data_04(betalsätt_data, file_list)
    data_05(forsäljning_data, file_list)
    data_06(forsäljning_data, file_list)
    data_07(forsäljning_data, file_list)
    data_08(forsäljning_data, file_list)
    data_09(forsäljning_data, file_list)
    data_10(forsäljning_data, file_list)
    data_11(forsäljning_data, file_list)
    data_12(moms_data, file_list)

    data_99(file_list)

    print(f"All files saved to folder: {export_folder}")

    # Add further export functionality here


def create_resulting_files(forsäljning_data, target_folder):
    created_files = []

    # Ensure the target folder exists
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # Get the current timestamp in Stockholm timezone
    stockholm_tz = pytz.timezone("Europe/Stockholm")
    timestamp = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

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


def map_serie_to_file_name(serie_value):
    """Map the 'Serie' value to a file name(ignoring the first two characters)"""
    return f"T0{serie_value[2:]}"


def data_00(file_list):
    header_row = ["00", "20250111_001", "1.0.0"]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in header_row]
            f.write(",".join(quoted_row) + "\n")


def data_01_02(följesedlar_data, file_list):
    file_data = {}
    current_number = None

    # Group data by "Nummer"
    grouped_data = följesedlar_data.groupby("Nummer")

    for number, group in grouped_data:
        # Find the first row with missing `article_id`
        missing_article_row = group[group["Referens"].isna() | (group["Referens"] == "")].head(1)

        # Determine the reference value based on missing `article_id`
        if not missing_article_row.empty:
            reference = missing_article_row.iloc[0]["Benämning"]  # Take the "Benämning" from the first missing "Referens"
        else:
            reference = ""  # No missing article_id, set reference to empty string

        # Create the "01" row (unique per "Nummer")
        first_row = group.iloc[0]
        shop_id = first_row["Butikskod"]
        cash_register_id = first_row["KassaId"]
        customer_id = first_row["Kundkod"]
        date = first_row["Dok.datum"]
        receipt_id = first_row["Nummer"]
        seller_id = first_row["Anställd"]

        # Find the matching file in file_list
        serie = first_row["Serie"]
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(
                f"Warning: Target file {target_file_partial} not found in file_list. Skipping group."
            )
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        mapped_row_01 = [
            "01",
            shop_id,
            cash_register_id,
            customer_id,
            date,
            reference,  # Set reference from missing "Referens" row or empty string
            receipt_id,
            seller_id,
        ]
        file_data[matching_file].append(mapped_row_01)

        # Create "02" rows only for rows where article_id is present
        for _, row in group.iterrows():
            article_id = row["Referens"]
            if pd.isna(article_id) or article_id == "":
                # Skip this row if article_id is missing
                continue

            # Create "02" row for valid article_id
            mapped_row_02 = [
                "02",
                row["Benämning"],
                article_id,
                row["Ant."],
                row["Pris "],
                row["EnhetsprisExMoms"],
                row["Total moms"],
                row["Rabatt"],
            ]
            file_data[matching_file].append(mapped_row_02)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")

def data_03(försäljning_data, file_list):

    for _, row in försäljning_data.iterrows():
        # Mapped values for 03
        # 03 Mapped values
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

    mapped_row_03 = ["03", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_03]
            f.write(",".join(quoted_row) + "\n")

def data_04(betalsätt_data, file_list):
    file_data = {}  # To store rows for each matching file
    betalmedel_sums = {}  # Store sums for each matching file and betalmedel
    processed_betalmedel_for_number = {}  # Track processed betalmedel per file and number

    for _, row in betalsätt_data.iterrows():
        serie = row["Serie"]
        number = row["Nummer"]
        dokumenttyp = row["Dokumenttyp"]

        # 04 Mapped values
        konto = row["Dok.Id"]
        betalmedel = row["Betalmedel"]
        belopp_value = str(row["Belopp"]).replace(".", "")
        debetbelopp = float(belopp_value.replace(",", "."))
        kreditbelopp = float(str(row["Belopp"]).replace(",", "."))

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
            betalmedel_sums[matching_file] = {}
            processed_betalmedel_for_number[matching_file] = {}

        if number not in processed_betalmedel_for_number[matching_file]:
            processed_betalmedel_for_number[matching_file][number] = set()

        # Handle sums based on the row's document type
        if betalmedel not in processed_betalmedel_for_number[matching_file][number]:
            if dokumenttyp == "Sale receipt":
                if betalmedel not in betalmedel_sums[matching_file]:
                    betalmedel_sums[matching_file][betalmedel] = {"debet": 0, "kredit": 0}
                betalmedel_sums[matching_file][betalmedel]["debet"] += debetbelopp
            elif dokumenttyp == "Sale receipt return":
                if betalmedel not in betalmedel_sums[matching_file]:
                    betalmedel_sums[matching_file][betalmedel] = {"debet": 0, "kredit": 0}
                betalmedel_sums[matching_file][betalmedel]["kredit"] += abs(kreditbelopp)

            # Mark this betalmedel as processed for the current number
            processed_betalmedel_for_number[matching_file][number].add(betalmedel)

    # Add the "04" rows based on stored sums for each matching file
    for matching_file, sums_per_file in betalmedel_sums.items():
        for betalmedel, sums in sums_per_file.items():
            konto = row["Bokföringssuffix"]  # This assumes "Bokföringssuffix" is the same for all rows
            debetbelopp = int(sums["debet"] * 100)
            kreditbelopp = int(sums["kredit"] * 100)
            mapped_row_04 = ["04", konto, betalmedel, str(debetbelopp), str(kreditbelopp)]
            file_data[matching_file].append(mapped_row_04)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")

def data_05(försäljning_data, file_list):

    for _, row in försäljning_data.iterrows():
        # Mapped values for 05
        # 03 Mapped values
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

    mapped_row_05 = ["05", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_05]
            f.write(",".join(quoted_row) + "\n")

#TODO kolla så att alla värden ser rätt ut jämfört med 001 filen. T.ex 12% = 1200 istället(just denna är fixad)
def data_06(försäljning_data, file_list):
    file_data = {}  # Dictionary to store rows per matching file

    for _, row in försäljning_data.iterrows():
        # Mapped values
        serie = row["Serie"]
        artikelNr = row["Referens"]
        antal = row["Enh.1"]
        pris = row["Pris "]  # Price is correct as-is
        tid = format_time(row["Timme"])  # Keep format_time function for formatting
        säljare = row["Anställd"]
        moms = row["Moms"].replace("%", "00")  # This is correct as-is
        doktyp = row["Dokumenttyp"]

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
            file_data[matching_file] = []  # Create an entry for this file

        # Negate the quantity for "Sale receipt return"
        if doktyp == "Sale receipt return":
            antal = -antal

        # Add the row to the corresponding file's data
        mapped_row_06 = ["06", artikelNr, antal, pris, tid, säljare, moms]
        file_data[matching_file].append(mapped_row_06)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")

def data_07(försäljning_data, file_list):

    for _, row in försäljning_data.iterrows():
        # Mapped values for 07
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

    mapped_row_07 = ["07", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_07]
            f.write(",".join(quoted_row) + "\n")


def data_08(försäljning_data, file_list):
    file_data = {}

    # Dictionary to store varugrupp data per file
    varugrupp_data = {}

    for _, row in försäljning_data.iterrows():
        serie = row["Serie"]
        antal = int(row["Enh.1"])  # Convert Enh.1 to integer
        pris = float(row["Netto"].replace(".", ""))  # Handle formatting for Netto
        moms = row["Moms"]
        varugrupp = row["Varugruppskod"]
        doktyp = row["Dokumenttyp"]

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

        # Create a separate varugrupp_data dictionary for each matching file
        if matching_file not in varugrupp_data:
            varugrupp_data[matching_file] = {}

        # Aggregate data for varugrupp within the matching file
        if varugrupp not in varugrupp_data[matching_file]:
            varugrupp_data[matching_file][varugrupp] = {"antal": 0, "total_pris": 0}

        # Negate for "Sale receipt return"
        if doktyp == "Sale receipt return":
            antal = -antal
            pris = -pris

        # Sum the values for the matching file and varugrupp
        varugrupp_data[matching_file][varugrupp]["antal"] += antal
        varugrupp_data[matching_file][varugrupp]["total_pris"] += pris

    # Map aggregated data into rows for the file
    for target_file, rows in file_data.items():
        for varugrupp, data in varugrupp_data[target_file].items():
            mapped_row_08 = [
                "08",
                varugrupp,
                data["antal"],  # Total quantity
                data["total_pris"],  # Total price
                moms,  # Format moms with commas
            ]
            rows.append(mapped_row_08)

        # Write the aggregated rows to the corresponding file
        with open(target_file, "a") as f:
            for row in rows:
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_09(försäljning_data, file_list):

    for _, row in försäljning_data.iterrows():
        # Mapped values for 09
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

    mapped_row_09 = ["09", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_09]
            f.write(",".join(quoted_row) + "\n")


def data_10(försäljning_data, file_list):
    file_data = {}
    time_interval_data = {}  # Dictionary to store data for each file

    for _, row in försäljning_data.iterrows():
        serie = row["Serie"]
        antal = int(row["Enh.1"])  # Amount (Enh.1)
        pris = float(row["Netto"].replace(".", ""))  # Convert Netto to float and handle formatting
        tid = row["Timme"]  # Time in "HH:mm:ss"

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            continue

        if matching_file not in file_data:
            file_data[matching_file] = []

        # Create a separate time_interval_data dictionary for each matching file
        if matching_file not in time_interval_data:
            time_interval_data[matching_file] = {}

        # Convert time to interval format
        hour, minute, _ = tid.split(":")
        hour = int(hour)
        start_time = f"{hour}.00"
        end_time = f"{hour + 1}.00" if hour + 1 < 24 else "0.00"
        time_interval = f"{start_time} - {end_time}"

        # Add to the appropriate file's time interval data
        if time_interval not in time_interval_data[matching_file]:
            time_interval_data[matching_file][time_interval] = {"antal": 0, "total_pris": 0}

        # Sum amounts and prices for the time interval
        time_interval_data[matching_file][time_interval]["antal"] += antal
        time_interval_data[matching_file][time_interval]["total_pris"] += pris

    # Map aggregated data to rows for each file
    for target_file, time_data in time_interval_data.items():
        for time_interval, data in time_data.items():
            mapped_row_10 = [
                "10",
                time_interval,
                data["antal"],  # Total amount for the interval
                data["total_pris"],  # Total price for the interval
            ]
            file_data[target_file].append(mapped_row_10)

        # Write to the corresponding file
        with open(target_file, "a") as f:
            for row in file_data[target_file]:
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_11(försäljning_data, file_list):

    file_data = {}

    for _, row in försäljning_data.iterrows():

        # Mapped values
        serie = row["Serie"]

        # Mapped values for 11
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if matching_file not in file_data:
            file_data[matching_file] = []

        mapped_row_11 = ["11", butiks_nr, kassa_nr, datum]
        file_data[matching_file].append(mapped_row_11)

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_11]
            f.write(",".join(quoted_row) + "\n")

# TODO Se till att pappa lägger till Serie i Moms filen, och gör det först i ordningen. Kolla också att att värdena är i rätt format
def data_12(moms_data, file_list):
    file_data = {}  # Dictionary to store rows for each matching file

    for _, row in moms_data.iterrows():
        # Mapped values
        serie = row["Serie"]
        moms = row["Moms"]
        basbelopp = row["Basbelopp"]
        moms_2 = row.iloc[5]  # You use the 5th column (adjust if needed)
        total_belopp = row["Totalbelopp"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(f"Warning: Target file {target_file_partial} not found in file_list. Skipping row.")
            continue

        # Initialize the list for this matching file if not already present
        if matching_file not in file_data:
            file_data[matching_file] = []

        # Add the row to the corresponding matching file
        mapped_row_12 = ["12", moms, basbelopp, moms_2, total_belopp]
        file_data[matching_file].append(mapped_row_12)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_99(file_list):
    footer_row = ["99"]

    for target_file in file_list:
        # Append the footer row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in footer_row]
            f.write(",".join(quoted_row) + "\n")

def format_time(tid):
        hour, minute, _ = tid.split(":")
        return f"{hour}{minute}"
