import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz
import time
import math
from decimal import Decimal


def export_action(file_paths):
    # Match file names to specific data objects
    forsäljning_data = None
    betalsätt_data = None
    följesedlar_data = None
    presentkort_data = None
    presentkort_sålda_data = None
    time.sleep(1)

    # print(f"Export action started with file paths: {file_paths}")

    for file_path in file_paths:
        file_name = os.path.basename(file_path)

        if "Försäljning" in file_name:
            forsäljning_data = pd.read_csv(
                file_path,
                sep=";",
                dtype={"Referens": str, "Netto": str, "Varugrupp": str},
                encoding="ISO-8859-1",
            )

        #     forsäljning_data.columns = [
        #     col.encode("latin1").decode("utf-8") for col in forsäljning_data.columns
        # ]

        elif "Betalsätt" in file_name:
            betalsätt_data = pd.read_csv(file_path, sep=";", encoding="ISO-8859-1")

        #     betalsätt_data.columns = [
        #     col.encode("latin1").decode("utf-8") for col in betalsätt_data.columns
        # ]

        elif "Följesedlar" in file_name:
            följesedlar_data = pd.read_csv(
                file_path,
                sep=";",
                dtype={"Netto": str, "Referens": str},
                encoding="ISO-8859-1",
            )

        #     följesedlar_data.columns = [
        #     col.encode("latin1").decode("utf-8") for col in följesedlar_data.columns
        # ]

        elif "Moms" in file_name:
            moms_data = pd.read_csv(
                file_path, sep=";", dtype={"Totalbelopp": str}, encoding="ISO-8859-1"
            )

        #     moms_data.columns = [
        #     col.encode("latin1").decode("utf-8") for col in moms_data.columns
        # ]

        elif "Presentkort_used" in file_name:
            presentkort_data = pd.read_csv(
                file_path, sep=";", dtype={"Belopp": str}, encoding="ISO-8859-1"
            )

            presentkort_data.columns = [
                col.encode("latin1").decode("utf-8") for col in presentkort_data.columns
            ]

        elif "Presentkort_sold" in file_name:
            presentkort_sålda_data = pd.read_csv(
                file_path,
                sep=";",
                dtype={"Belopp": str, "Kort": str},
                encoding="ISO-8859-1",
            )

        #     presentkort_sålda_data.columns = [
        #     col.encode("latin1").decode("utf-8") for col in presentkort_sålda_data.columns
        # ]

    if forsäljning_data is None or betalsätt_data is None or moms_data is None:
        raise ValueError("One or more required files are missing from the file paths.")

    # Only raise a warning if `presentkort_sålda_data` is missing
    if presentkort_sålda_data is None:
        print("Warning: 'Presentkort_sold.csv' is missing. Proceeding without it.")

    if presentkort_data is None:
        print("Warning: 'Presentkort_used.csv' is missing. Proceeding without it.")

    if följesedlar_data is None:
        print("Warning: 'Följesedlar.csv' is missing. Proceeding without it.")

    file_path = file_paths[0]
    # base_dir = os.path.dirname(file_path)
    export_folder = os.path.join("C:/winbag_export")

    butikskod_serie_map = create_butikskod_serie_map(forsäljning_data)

    file_list = create_resulting_files(forsäljning_data, export_folder, butikskod_serie_map)

    # print(f"Serie to butikskod map: {butikskod_serie_map}")

    data_00(file_list)
    data_01_02(följesedlar_data, file_list, butikskod_serie_map)
    data_03(forsäljning_data, file_list)
    data_04(betalsätt_data, file_list, presentkort_sålda_data, butikskod_serie_map)
    data_04_följesedlar(följesedlar_data, file_list, butikskod_serie_map)
    data_04_presentkort(presentkort_data, file_list, butikskod_serie_map)
    data_05(forsäljning_data, file_list)
    data_06(forsäljning_data, file_list, butikskod_serie_map)
    data_07(forsäljning_data, file_list)
    data_08(forsäljning_data, file_list, butikskod_serie_map)
    data_09(forsäljning_data, file_list)
    data_10(forsäljning_data, file_list, butikskod_serie_map)
    data_11(forsäljning_data, file_list, butikskod_serie_map)
    data_12(moms_data, file_list, butikskod_serie_map)

    data_99(file_list)

    print(f"All files saved to folder: {export_folder}")

    # Add further export functionality here


def create_resulting_files(forsäljning_data, target_folder, butikskod_serie_map):
    created_files = []

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    #print(f"Sales date: {sales_date}")
    for _, row in forsäljning_data.iterrows():
        sales_date = row["Dok.datum"]


    time = datetime.now(ZoneInfo("Europe/Stockholm")).strftime("%H%M")
    formated_sales_date = format_sales_date(sales_date)

    for butikskod, serie in butikskod_serie_map.items():
        file_name = f"0{butikskod}_000_{formated_sales_date}_{time}.TXT"
        file_path = os.path.join(target_folder, file_name)

        with open(file_path, "w") as f:
            pass

        created_files.append(file_path)

    return created_files


def map_serie_to_file_name(serie_value, butikskod_serie_map):
    """
    Map the 'Serie' value to its corresponding 'Butikskod' key based on the map.
    """

    # If the serie_value starts with "AV", transform it
    if serie_value.startswith("AV"):
        first_digit = serie_value[2]  # First number after AV
        remaining_part = serie_value[3:]  # Remaining numbers
        serie_value = f"T{first_digit}0{remaining_part}"  # Insert extra 0
        # print(f"Transformed Serie value: {serie_value}")

    # Reverse lookup: find the key where the value matches the serie_value
    butikskod_value = next(
        (key for key, value in butikskod_serie_map.items() if value == serie_value),
        None,  # Avoid crashing if no match is found
    )

    if butikskod_value is None:
        raise ValueError(f"Serie value {serie_value} not found in mapping.")

    return f"{butikskod_value}"


def data_00(file_list):
    header_row = ["00", "20250111_001", "1.0.0"]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in header_row]
            f.write(",".join(quoted_row) + "\n")


def data_01_02(följesedlar_data, file_list, butikskod_serie_map):
    file_data = {}
    current_number = None

    # print(följesedlar_data.columns.tolist())

    if följesedlar_data is not None:
        # Group data by "Nummer"
        grouped_data = följesedlar_data.groupby("Nummer")
        for number, group in grouped_data:
            # Find the first row with missing `article_id`
            missing_article_row = group[
                group["Referens"].isna() | (group["Referens"] == "")
            ].head(1)

            # Determine the reference value based on missing `article_id`
            if not missing_article_row.empty:
                reference = missing_article_row.iloc[0][
                    "Benämning"
                ]  # Take the "Benämning" from the first missing "Referens"
            else:
                reference = ""  # No missing article_id, set reference to empty string

            #print(följesedlar_data.columns.tolist())

            # Create the "01" row (unique per "Nummer")
            first_row = group.iloc[0]
            shop_id = first_row["ButikskodWinbag"]  
            print(f"Shop ID: {shop_id}")
            customer_id = first_row["Kundkod"]
            date = first_row["Dok.datum"]
            date = datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d")
            receipt_id = first_row["Nummer"]
            seller_id = first_row["Anställd"]

            # Find the matching file in file_list
            serie = first_row["Serie"]
            target_file = map_serie_to_file_name(serie, butikskod_serie_map)

            # print(f"Target file: {target_file}")
            target_file_partial = f"{target_file}"
            matching_file = next(
                (f for f in file_list if target_file_partial in f), None
            )

            if not matching_file:
                print(
                    f"Warning: Target file {target_file_partial} not found in file_list. Skipping group."
                )
                continue

            if matching_file not in file_data:
                file_data[matching_file] = []

            mapped_row_01 = [
                "01",
                f"0{shop_id}",
                f"0{shop_id}",
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

                antal = row["Ant."]
                pris = format_value_as_integer_string(row["Pris "])
                enhetspris = round(float(row["EnhetsprisExMoms"].replace(",", ".")), 2)
                if antal < 0:
                    enhetspris = f"-{enhetspris}"
                    pris = f"-{pris}"

                # Create "02" row for valid article_id
                mapped_row_02 = [
                    "02",
                    "0",  # TODO Check if this should be something else if its a reference
                    article_id,
                    format_antal_as_integer_string(row["Ant."]),
                    pris,
                    format_value_as_integer_string(enhetspris),
                    row["Moms"].replace("%", "00").replace(" ", ""),
                    format_rabatt_nr(row["Rabatt"]),
                    format_value_as_integer_string(enhetspris),
                ]
                file_data[matching_file].append(mapped_row_02)
    else:
        print(
            "Warning: 'Följesedlar.csv' data is missing. Skipping följesedlar processing."
        )

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
        butiks_nr = format_kassa_id(row["ButikskodWinbag"])
        kassa_nr = format_kassa_id(row["ButikskodWinbag"])
        raw_datum = row["Dok.datum"]
        datum = datetime.strptime(raw_datum, "%d/%m/%Y").strftime("%Y-%m-%d")

    mapped_row_03 = ["03", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_03]
            f.write(",".join(quoted_row) + "\n")


def data_04(betalsätt_data, file_list, presentkort_sålda, butikskod_serie_map):
    file_data = {}  # To store rows for each matching file
    betalmedel_sums = {}  # Store sums for each matching file and betalmedel
    processed_betalmedel_for_number = (
        {}
    )  # Track processed betalmedel per file and number
    suffix_mapping = {}  # Store the Bokföringssuffix for each betalmedel per file

    # Preprocess presentkort_sålda for easy lookup
    presentkort_sålda_data = {}
    if presentkort_sålda is not None:
        for _, row in presentkort_sålda.iterrows():
            kort = str(row["Kort"])
            betalmedel = row["Betalmedel"]
            belopp = float(str(row["Belopp"]).replace(".", "").replace(",", "."))

            if kort != "nan":
                if betalmedel not in presentkort_sålda_data:
                    presentkort_sålda_data[betalmedel] = 0
                presentkort_sålda_data[betalmedel] += belopp
    else:
        print(
            "Warning: 'Presentkort_sold.csv' data is missing. Skipping presentkort_sold processing."
        )

    for _, row in betalsätt_data.iterrows():
        serie = row["Serie"]
        number = row["Nummer"]
        kod_dokumenttyp = row["Kod för dokumenttyp"]

        # 04 Mapped values
        konto = row["Dok.Id"]
        betalmedel = row["Betalmedel"]
        belopp_value = str(row["Belopp"]).replace(".", "")
        debetbelopp = float(belopp_value.replace(",", "."))
        kreditbelopp = float(belopp_value.replace(",", "."))
        bokföringssuffix = row["Bokföringssuffix"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, butikskod_serie_map)
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
            suffix_mapping[matching_file] = {}

        if number not in processed_betalmedel_for_number[matching_file]:
            processed_betalmedel_for_number[matching_file][number] = set()

        # Store the bokföringssuffix for this betalmedel
        if betalmedel not in suffix_mapping[matching_file]:
            suffix_mapping[matching_file][betalmedel] = bokföringssuffix

        # Add presentkort_sålda belopp to the betalmedel as debet
        # Add presentkort_sålda belopp to the betalmedel as debet
        if betalmedel in presentkort_sålda_data:
            if betalmedel not in betalmedel_sums[matching_file]:
                betalmedel_sums[matching_file][betalmedel] = {"debet": 0, "kredit": 0}

                betalmedel_sums[matching_file][betalmedel][
                    "debet"
                ] += presentkort_sålda_data[betalmedel]

        # Handle sums based on the row's document type
        if betalmedel not in processed_betalmedel_for_number[matching_file][number]:
            if kod_dokumenttyp == 1:
                if betalmedel not in betalmedel_sums[matching_file]:
                    betalmedel_sums[matching_file][betalmedel] = {
                        "debet": 0,
                        "kredit": 0,
                    }

                betalmedel_sums[matching_file][betalmedel]["debet"] += debetbelopp

            elif kod_dokumenttyp == 3:
                if betalmedel not in betalmedel_sums[matching_file]:
                    betalmedel_sums[matching_file][betalmedel] = {
                        "debet": 0,
                        "kredit": 0,
                    }
                betalmedel_sums[matching_file][betalmedel]["kredit"] += abs(
                    kreditbelopp
                )

            # Mark this betalmedel as processed for the current number
            processed_betalmedel_for_number[matching_file][number].add(betalmedel)

    # Add the "04" rows based on stored sums for each matching file
    for matching_file, sums_per_file in betalmedel_sums.items():
        for betalmedel, sums in sums_per_file.items():
            konto = suffix_mapping[matching_file][betalmedel]
            debetbelopp = format_value_as_integer_string(sums["debet"])
            kreditbelopp = format_value_as_integer_string(sums["kredit"])
            mapped_row_04 = [
                "04",
                konto,
                betalmedel,
                str(debetbelopp),
                str(kreditbelopp),
            ]
            file_data[matching_file].append(mapped_row_04)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_04_följesedlar(följesedlar_data, file_list, butikskod_serie_map):
    file_data = {}  # To store rows for each matching file
    sums_per_file = {}  # Store sums for each matching file
    processed_numbers_for_file = {}  # Track processed numbers per file
    suffix_mapping = {}  # Store the Bokföringssuffix for each betalmedel per file

    if följesedlar_data is not None:
        for _, row in följesedlar_data.iterrows():
            serie = row["Serie"]
            number = row["Nummer"]
            bokföringssuffix = row["Bokföringssuffix"]

            # 04 Mapped values
            konto = row["Dok.Id"]
            pris_value = float(row["Netto"].replace(".", "").replace(",", "."))
            antal = row["Ant."]

            if antal < 0:
                pris_value = -pris_value

            # Determine debet and kredit based on sign of pris_value
            debetbelopp = pris_value if pris_value > 0 else 0.0
            kreditbelopp = abs(pris_value) if pris_value < 0 else 0.0

            # Find the matching file in file_list
            target_file = map_serie_to_file_name(serie, butikskod_serie_map)
            target_file_partial = f"{target_file}"
            matching_file = next(
                (f for f in file_list if target_file_partial in f), None
            )
            


            if not matching_file:
                print(
                    f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
                )
                continue

            if matching_file not in file_data:
                file_data[matching_file] = []
                sums_per_file[matching_file] = {"debet": 0, "kredit": 0}
                processed_numbers_for_file[matching_file] = set()
                suffix_mapping[matching_file] = None

            # Update sums
            sums_per_file[matching_file]["debet"] += debetbelopp
            sums_per_file[matching_file]["kredit"] += kreditbelopp

            if suffix_mapping[matching_file] is None:
                suffix_mapping[matching_file] = bokföringssuffix

            # Mark this number as processed
            processed_numbers_for_file[matching_file].add(number)
    else:
        print(
            "Warning: 'Följesedlar.csv' data is missing. Skipping följesedlar processing."
        )

    # Add the "04" rows based on stored sums for each matching file
    for matching_file, sums in sums_per_file.items():
        konto = suffix_mapping[matching_file]
        if konto is None:
            print(f"Warning: No Bokföringssuffix found for file {matching_file}")
            continue
        debetbelopp = format_value_as_integer_string(sums["debet"])
        kreditbelopp = format_value_as_integer_string(sums["kredit"])
        mapped_row_04 = [
            "04",
            konto,
            "Följesedlar",
            str(debetbelopp),
            str(kreditbelopp),
        ]
        file_data[matching_file].append(mapped_row_04)

    # Write each set of rows to its corresponding file
    for target_file, rows in file_data.items():
        with open(target_file, "a") as f:
            for row in rows:
                # Add quotes around each value
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_04_presentkort(presentkort_data, file_list, serie_butikskod_map):
    file_data = {}
    sums_per_file = {}
    account_mapping = {}

    if presentkort_data is not None:
        for _, row in presentkort_data.iterrows():
            butikskod = row["Butikskod"]

            serie = serie_butikskod_map.get(butikskod)
            presentkortskonto = row["Presentkortskonto"]

            # Extract transaction type and value
            typ_av_transaktion = row["Kod för kundkortstransaktioner"]
            pris_value = float(row["Belopp"].replace(".", "").replace(",", "."))

            # Determine positive_value and negative_value based on transaction type
            positive_value = abs(pris_value) if typ_av_transaktion == 5 else 0.0
            negative_value = pris_value if typ_av_transaktion == 2 else 0.0

            # Find the matching file in file_list
            target_file = map_serie_to_file_name(serie, serie_butikskod_map)
            target_file_partial = f"{target_file}"
            matching_file = next(
                (f for f in file_list if target_file_partial in f), None
            )

            if not matching_file:
                print(
                    f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
                )
                continue

            if matching_file not in file_data:
                file_data[matching_file] = []
                sums_per_file[matching_file] = {"positive": 0, "negative": 0}
                account_mapping[matching_file] = None

            # Update sums
            sums_per_file[matching_file]["positive"] += positive_value
            sums_per_file[matching_file]["negative"] += negative_value

            if account_mapping[matching_file] is None:
                account_mapping[matching_file] = presentkortskonto

    else:
        print(
            "Warning: 'Presentkort_used.csv' data is missing. Skipping presentkort processing."
        )

    # Add the "04" rows based on stored sums for each matching file
    for matching_file, sums in sums_per_file.items():
        konto = account_mapping[matching_file]
        if konto is None:
            print(f"Warning: No Presentkortskonto found for file {matching_file}")
            continue
        positive_value = format_value_as_integer_string(sums["positive"])
        negative_value = format_value_as_integer_string(sums["negative"])
        mapped_row_04 = [
            "04",
            konto,
            "Presentkort",
            "0",
            str(positive_value),
        ]
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
        # 05 Mapped values
        butiks_nr = format_kassa_id(row["ButikskodWinbag"])
        kassa_nr = format_kassa_id(row["ButikskodWinbag"])
        raw_datum = row["Dok.datum"]
        datum = datetime.strptime(raw_datum, "%d/%m/%Y").strftime("%Y-%m-%d")

    mapped_row_05 = ["05", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_05]
            f.write(",".join(quoted_row) + "\n")


def data_06(försäljning_data, file_list, butikskod_serie_map):
    file_data = {}  # Dictionary to store rows per matching file

    for _, row in försäljning_data.iterrows():
        # Mapped values
        serie = row["Serie"]
        artikelNr = row["Referens"]
        antal = row["Enh.1"]
        pris = row["Pris "]  # Price is correct as-is
        tid = format_time(row["Timme"])  # Keep format_time function for formatting
        säljare = row["Anställd"]
        moms = row["Moms"].replace("%", "00").replace(" ", "")  # This is correct as-is
        kod_doktyp = row["Kod för dokumenttyp"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, butikskod_serie_map)
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
        if kod_doktyp == 3:
            antal = -antal

        # Add the row to the corresponding file's data
        mapped_row_06 = [
            "06",
            artikelNr,
            format_antal_as_integer_string(antal),
            format_value_as_integer_string(pris),
            tid,
            säljare,
            moms,
        ]
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
        butiks_nr = format_kassa_id(row["ButikskodWinbag"])
        kassa_nr = format_kassa_id(row["ButikskodWinbag"])
        raw_datum = row["Dok.datum"]
        datum = datetime.strptime(raw_datum, "%d/%m/%Y").strftime("%Y-%m-%d")

    mapped_row_07 = ["07", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_07]
            f.write(",".join(quoted_row) + "\n")


def data_08(försäljning_data, file_list, butikskod_serie_map):
    file_data = {}

    # Dictionary to store varugrupp data per file
    varugrupp_data = {}

    for _, row in försäljning_data.iterrows():
        serie = row["Serie"]
        antal = int(row["Enh.1"])  # Convert Enh.1 to integer
        pris = float(
            row["Netto"].replace(".", "").replace(",", ".")
        )  # Handle formatting for Netto
        moms = row["Moms"].replace("%", "00").replace(" ", "")
        varugrupp = row["Varugruppskod"]
        if varugrupp and not math.isnan(float(varugrupp)):
            varugrupp = int(float(varugrupp))
        else:
            # Handle the NaN case appropriately
            varugrupp = "NaN"  # or another default value or action

        kod_doktyp = row["Kod för dokumenttyp"]

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, butikskod_serie_map)
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
        if kod_doktyp == 3:
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
                format_antal_as_integer_string(data["antal"]),  # Total quantity
                format_value_as_integer_string(data["total_pris"]),  # Total price
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
        butiks_nr = format_kassa_id(row["ButikskodWinbag"])
        kassa_nr = format_kassa_id(row["ButikskodWinbag"])
        raw_datum = row["Dok.datum"]
        datum = datetime.strptime(raw_datum, "%d/%m/%Y").strftime("%Y-%m-%d")

    mapped_row_09 = ["09", butiks_nr, kassa_nr, datum]

    for target_file in file_list:
        # Append the header row to each file
        with open(target_file, "a") as f:
            quoted_row = [f'"{value}"' for value in mapped_row_09]
            f.write(",".join(quoted_row) + "\n")


def data_10(försäljning_data, file_list, butikskod_serie_map):
    file_data = {}
    time_interval_data = {}  # Dictionary to store data for each file

    for _, row in försäljning_data.iterrows():
        serie = row["Serie"]
        antal = int(row["Enh.1"])  # Amount (Enh.
        pris = float(
            row["Netto"].replace(".", "").replace(",", ".")
        )  # Convert Netto to float and handle formatting
        tid = row["Timme"]  # Time in "HH:mm:ss"

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, butikskod_serie_map)
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
            time_interval_data[matching_file][time_interval] = {
                "antal": 0,
                "total_pris": 0,
            }

        # Sum amounts and prices for the time interval
        time_interval_data[matching_file][time_interval]["antal"] += antal
        time_interval_data[matching_file][time_interval]["total_pris"] += pris

    # Map aggregated data to rows for each file
    for target_file, time_data in time_interval_data.items():
        for time_interval, data in time_data.items():
            mapped_row_10 = [
                "10",
                time_interval,
                format_value_as_integer_string(
                    data["antal"]
                ),  # This one should be 2 decimals
                format_value_as_integer_string(
                    data["total_pris"]
                ),  # Total price for the interval
            ]
            file_data[target_file].append(mapped_row_10)

        # Write to the corresponding file
        with open(target_file, "a") as f:
            for row in file_data[target_file]:
                quoted_row = [f'"{value}"' for value in row]
                f.write(",".join(quoted_row) + "\n")


def data_11(försäljning_data, file_list, butikskod_serie_map):

    file_data = {}

    for _, row in försäljning_data.iterrows():

        # Mapped values
        serie = row["Serie"]

        # Mapped values for 11
        butiks_nr = format_kassa_id(row["ButikskodWinbag"])
        kassa_nr = format_kassa_id(row["ButikskodWinbag"])
        raw_datum = row["Dok.datum"]
        datum = datetime.strptime(raw_datum, "%d/%m/%Y").strftime("%Y-%m-%d")

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, butikskod_serie_map)
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


def data_12(moms_data, file_list, serie_butikskod_map):
    file_data = {}  # Dictionary to store rows for each matching file

    for _, row in moms_data.iterrows():
        # Get the "Butikskod" and map it to "Serie"
        butikskod = row["Butikskod"]
        serie = serie_butikskod_map.get(butikskod)

        if not serie:
            print(
                f"Warning: Butikskod {butikskod} not found in serie_butikskod_map. Skipping row."
            )
            continue

        # Mapped values
        moms = row["Moms"].replace("%", "00").replace(" ", "")
        basbelopp = row["Basbelopp"].replace(".", "").replace(",", ".")
        moms_2 = row["Moms_2"].replace(".", "").replace(",", ".")
        total_belopp = row["Totalbelopp"].replace(".", "").replace(",", ".")

        # Find the matching file in file_list
        target_file = map_serie_to_file_name(serie, serie_butikskod_map)
        target_file_partial = f"{target_file}"
        matching_file = next((f for f in file_list if target_file_partial in f), None)

        if not matching_file:
            print(
                f"Warning: Target file {target_file_partial} not found in file_list. Skipping row."
            )
            continue

        # Initialize the list for this matching file if not already present
        if matching_file not in file_data:
            file_data[matching_file] = []

        basbelopp = format_value_as_integer_string(basbelopp)
        moms_2 = format_value_as_integer_string(moms_2)
        total_belopp = format_value_as_integer_string(total_belopp)

        # Add the row to the corresponding matching file
        mapped_row_12 = [
            "12",
            moms,
            basbelopp,
            moms_2,
            total_belopp,
        ]
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


def create_butikskod_serie_map(forsäljning_data):
    butikskod_serie_map = (
        {}
    )  # Dictionary to store "Serie" as key and "Butikskod" as value

    for _, row in forsäljning_data.iterrows():
        serie = row["Serie"]
        butikskod = row["Butikskod"]

        # Add to dictionary, assuming "Serie" is unique
        butikskod_serie_map[butikskod] = serie

    return butikskod_serie_map


def format_value_as_integer_string(value):
    value_str = str(value).replace(",", ".")  # In case commas are used for decimals
    if "." in value_str:
        # Remove the decimal and append missing digits if necessary
        integer_part, decimal_part = value_str.split(".")
        decimal_part = decimal_part.ljust(2, "0")  # Ensure at least 2 digits
        formatted_value = integer_part + decimal_part
    else:
        # No decimal point, just add "00"
        formatted_value = value_str + "00"

    return formatted_value


def format_antal_as_integer_string(value):
    value_str = str(value).replace(",", ".")  # In case commas are used for decimals
    if "." in value_str:
        # Remove the decimal and append missing digits if necessary
        integer_part, decimal_part = value_str.split(".")
        decimal_part = decimal_part.ljust(2, "0")  # Ensure at least 2 digits
        formatted_value = integer_part + decimal_part
    else:
        # No decimal point, just add "000"
        formatted_value = value_str + "000"

    return formatted_value


def format_rabatt_nr(rabatt_nr):
    if rabatt_nr == 0:
        return "000"
    elif 1 <= rabatt_nr <= 9:
        return f"00{rabatt_nr}"
    elif 10 <= rabatt_nr <= 99:
        return f"0{rabatt_nr}"
    else:
        return str(rabatt_nr)


def format_kassa_id(kassa_id):
    if kassa_id < 10:
        return f"0{kassa_id}"
    else:
        return str(kassa_id)

def format_sales_date(sales_date):
    
    # Parse the combined date and time string into a datetime object
    date_obj = datetime.strptime(sales_date, "%d/%m/%Y")
    
    # Format the datetime object into the desired format: YYMMDD_HHMM
    return date_obj.strftime("%y%m%d")
