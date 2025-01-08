import os
import pandas as pd
from datetime import datetime
import pytz
import time


def export_action(file_paths):
    # Match file names to specific data objects
    forsäljning_data = None
    betalsätt_data = None
    följesedlar_data = None
    time.sleep(1)

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        if "Försäljning" in file_name:
            forsäljning_data = pd.read_csv(file_path, sep=";")
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
    data_03_04(betalsätt_data, file_list)
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

    for _, row in följesedlar_data.iterrows():

        serie = row["Serie"]
        number = row["Nummer"]

        # 01 Mapped values
        shop_id = row["Butikskod"]
        cash_register_id = row["KassaId"]
        customer_id = row["Kundkod"]
        date = row["Datum"]
        reference = row["Benämning"] # TODO Denna ska ändras, se post it kommentar
        receipt_id = row["Nummer"]
        seller_id = row["Anställd"]

        # 02 Mapped values
        article = row["Artikel"]
        article_id = row["Referens"]
        quantity = row["Ant."]
        brutto = row["Pris"]
        netto = row["Netto"] # TODO Denna saknas, pappa fixar(nettopris/exklusive moms)
        moms = row["Total moms"]
        discount = row["Total moms"]  # TODO Denna saknas, pappa fixar (Rabatt?)

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
        butiks_nr = row["KassaId"]
        kassa_nr = row["KassaId"]
        datum = row["Dok.datum"]

        # 04 Mapped values
        konto = row["Dok.Id"]
        betalmedel = row["Betalmedel"]
        debetbelopp = float(str(row["Belopp"]).replace(".", "").replace(",", ".")) # TODO Denna räknar inte helt rätt, t.ex. kort visar 1100 istället för 110 och swish är ca 150 fel
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

    # Only add a new "03" row if the "Nummer" changes
    if current_number == None:
        mapped_row_03 = ["03", butiks_nr, kassa_nr, datum]
        file_data[matching_file].append(mapped_row_03)
        current_number = number  # Update the current "Nummer"

    # # Add the "04" rows based on stored sums
    for betalmedel, sums in betalmedel_sums.items():
        konto = row["Bokföringssuffix"]
        debetbelopp = sums["debet"] # Kolla över denna den räknar inte rätt
        kreditbelopp = sums["kredit"]
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


def data_06(försäljning_data, file_list):

    file_data = {}

    for _, row in försäljning_data.iterrows():

        # Mapped values
        serie = row["Serie"]

        # Mapped values for odd rows
        artikelNr = row["Referens"]
        antal = row["Enh.1"]
        pris = row["Pris "]
        tid = row["Timme"]  # TODO Fel format, får 08:39:34 vill ha 0839(inga tecken, inga sekunder)
        säljare = row[
            "Anställd"
        ]
        moms = row["Moms"] # TODO Pappa tar bort den felaktiga momsen så att den riktiga(momssatsen) används istället

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

        if doktyp == "Sale receipt return":
            # Create the custom mapping for all rows
            antal = -antal  # Negate the quantity for return rows

        # Create the custom mapping for all rows
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

    # Dictionary to store the aggregated data for varugrupp
    varugrupp_data = {}

    for _, row in försäljning_data.iterrows():
        # Mapped values
        serie = row["Serie"]
        antal = row["Enh.1"]
        pris = row["Pris "]
        moms = row["Moms"] # TODO Pappa tar bort den felaktiga momsen så att den riktiga(momssatsen) används istället, måste även ta bort summeringen
        varugrupp = row["Varugruppskod"] # TODO Pappa lägger till en varugruppskod på artiklarna

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

        # If doktyp is "Sale receipt return", negate the quantity and price
        if doktyp == "Sale receipt return":
            antal = -antal  # Negate the quantity for return rows
            pris = -pris  # Negate the price for return rows

        # Aggregate the data for varugrupp
        if varugrupp not in varugrupp_data:
            varugrupp_data[varugrupp] = {"antal": 0, "total_pris": 0, "total_moms": 0}

        moms = moms.replace(",", ".")  # Replace comma with dot

        varugrupp_data[varugrupp]["antal"] += int(antal)
        varugrupp_data[varugrupp]["total_pris"] += float(pris)
        varugrupp_data[varugrupp]["total_moms"] += float(moms)

    # Now, map the aggregated data into rows for the file
    for target_file, rows in file_data.items():
        for varugrupp, data in varugrupp_data.items():
            data["total_moms"] = str(data["total_moms"]).replace(".", ",")

            mapped_row_08 = [
                "08",
                varugrupp,
                data["antal"],
                data["total_pris"],
                data["total_moms"],
            ]
            rows.append(mapped_row_08)

        # Write the aggregated rows to the corresponding file
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
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

    # Dictionary to store the aggregated data for each time interval
    time_interval_data = {}

    for _, row in försäljning_data.iterrows():
        # Mapped values
        serie = row["Serie"]
        antal = row["KassaId"]  # TODO Antal ska vara antal kunder dvs antal unika nummer på kvittona per tid(86, 87, 88)
        pris = row["Netto"] # TODO Uträkningen är helt fel
        tid = row["Timme"]  # Time value in "HH:mm" format

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

        # If doktyp is "Sale receipt return", negate the quantity and price
        if doktyp == "Sale receipt return":
            antal = -antal  # Negate the quantity for return rows
            pris = -pris  # Negate the price for return rows

        # Convert the time (tid) to the interval format
        time_parts = tid.split(":")
        hour = int(time_parts[0])

        # Any time between "x.00" and "x.59" belongs to the interval "x.00 - x+1.00"
        start_time = f"{hour}.00"
        end_time = (
            f"{hour + 1}.00" if hour + 1 < 24 else "0.00"
        )  # Wrap around to "0.00" at midnight

        time_interval = f"{start_time} - {end_time}"

        # Aggregate the data for each time interval
        # TODO Check how many times this happens, am I resetting to often?
        if time_interval not in time_interval_data:
            time_interval_data[time_interval] = {"antal": 0, "total_pris": 0}

        time_interval_data[time_interval]["antal"] += antal
        time_interval_data[time_interval][
            "total_pris"
        ] += pris  # Total price per interval

    # Now, map the aggregated data into rows for the file
    for target_file, rows in file_data.items():
        for time_interval, data in time_interval_data.items():
            mapped_row_10 = [
                "10",  # Identifier for 10 type
                time_interval,  # Time interval
                data["antal"],  # Total quantity for the interval
                data["total_pris"],  # Total price for the interval
            ]

            # Append the row to the list
            rows.append(mapped_row_10)

        # Write the aggregated rows to the corresponding file
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
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

# TODO Only the last 12 row is appended
def data_12(moms_data, file_list):

    file_data = {}

    for _, row in moms_data.iterrows():
        # Mapped values
        serie = row["Serie"]

        # Mapped values for 11
        moms = row["Moms"]
        basbelopp = row["Basbelopp"]
        moms_2 = row.iloc[4]
        total_belopp = row["Totalbelopp"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        mapped_row_12 = ["12", moms, basbelopp, moms_2, total_belopp]
        file_data[matching_file].append(mapped_row_12)

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_12]
            f.write(",".join(quoted_row) + "\n")


def data_99(file_list):
    footer_row = ["99"]

    for target_file in file_list:
        # Append the footer row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in footer_row]
            f.write(",".join(quoted_row) + "\n")
