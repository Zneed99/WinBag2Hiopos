import os
import time
import pytz
from datetime import datetime


def import_action(file_paths):
    """
    Takes a list of file paths, expects exactly one 'pcs.adm' file,
    and splits its contents into four new files based on the rules:

    1) '01' or '11'  --> first output file
    2) '02' or '22'  --> second output file
    3) '03' or '33'  --> check 6th column:
       - if empty    --> third output file
       - if not empty -> fourth output file
    """
    time.sleep(1)

    if not file_paths:
        print("No file paths provided to import_action.")
        return

    pcs_file_path = file_paths[0]
    if not os.path.exists(pcs_file_path):
        print(f"File does not exist: {pcs_file_path}")
        return

    # Define output file names in the same directory as pcs_file_path
    base_dir = os.path.dirname(pcs_file_path)
    import_folder = os.path.join(base_dir, "Imported Files")
    if not os.path.exists(import_folder):
        os.makedirs(import_folder)
        print(f"Created 'Imported Files' folder at {import_folder}")

    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    output1_path = os.path.join(import_folder, f"file_01_11.{current_time}.csv")
    output2_path = os.path.join(import_folder, f"file_artiklar.{current_time}.csv")
    output3_path = os.path.join(import_folder, f"file_huvudgrupp.{current_time}.csv")
    output4_path = os.path.join(import_folder, f"file_varugrupp.{current_time}.csv")

    try:
        with open(pcs_file_path, "r", encoding="cp1252") as pcs_in, open(
            output1_path, "w", encoding="cp1252"
        ) as out1, open(output2_path, "w", encoding="cp1252") as out2, open(
            output3_path, "w", encoding="cp1252"
        ) as out3, open(
            output4_path, "w", encoding="cp1252"
        ) as out4:

            for line in pcs_in:
                # Remove trailing newline/spaces
                clean_line = line.strip()
                if not clean_line:
                    continue  # skip empty lines

                row = clean_line.split(",")

                # We need at least one column to proceed
                if not row:
                    continue

                first_value = row[0].strip('"')  # remove leading/trailing quotes

                # 01 / 11 --> file 1
                if first_value in ("01", "11"):
                    tranformed_row = transform_01_11(row)
                    out1.write(tranformed_row + "\n")

                # 02 / 22 --> file 2
                elif first_value in ("02", "22"):
                    tranformed_row = transform_02_22(row)
                    out2.write(tranformed_row + "\n")

                # 03 / 33 --> check the 6th column
                elif first_value in ("03", "33"):
                    if len(row) >= 6 and row[5].strip('"'):
                        # There's a value in the 6th column
                        transformed_row = transform_varugrupp(row)
                        out4.write(transformed_row + "\n")
                    else:
                        # The 6th column is empty or doesn't exist
                        transformed_row = transform_huvudgrupp(row)
                        out3.write(transformed_row + "\n")

                else:
                    # Passes 00 and 99
                    pass

        print("import_action completed successfully.")

    except Exception as e:
        print(f"An error occurred in import_action: {e}")


def transform_01_11(row):
    """
    Given a row like:
       row[0] -> "01" or "11"
       row[3] -> "0398"
       row[4] -> "The Swedish Club"
       row[5] -> "Gullbergsstrandgata 6"
       row[6] -> "Leveranskunder hotell,rest mm"
    We return a string like:
       "F ; 0398 ; The Swedish Club ; Gullbergsstrandgata 6 ; Leveranskunder hotell,rest mm"
    or
       "T ; 0398 ; The Swedish Club ; Gullbergsstrandgata 6 ; Leveranskunder hotell,rest mm"
    depending on whether row[0] is "01" (F) or "11" (T).
    """

    # Safety check: make sure we have enough columns
    if len(row) < 7:
        return ""  # or raise an error/log it

    # print(f"First value: {row[0]}")

    # Determine T or F
    tf_value = "False" if row[0].strip('"') == "01" else "True"

    # Extract columns (strip() to remove accidental whitespace)
    code = row[3].strip('"')
    name = row[4].strip('"')
    address = row[5].strip('"')
    desc = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{tf_value} ; {code} ; {name} ; {address} ; {desc}"


def transform_02_22(row):
    """
    Given a row like:
       row[0] -> "02" or "22"
       row[3] -> "2"
       row[4] -> "Soppa & t�rtbit TA"
       row[6] -> "60"
       row[7] -> "63"
       row[8] -> "7500"
       row[9] -> "1200"
       row[10] -> "7500"
    """

    # Safety check: make sure we have enough columns
    if len(row) < 21:
        return ""  # or raise an error/log it

    # Determine T or F
    tf_value = "False" if row[0].strip('"') == "02" else "True"

    # Extract columns (strip() to remove accidental whitespace)
    code = row[3].strip('"')
    name = row[4].strip('"')
    streckkod = row[5].strip('"')
    value_1 = row[6].strip('"')
    value_2 = row[7].strip('"')
    price = row[8].strip('"').replace("00", "").lstrip("0") or "0"
    moms = row[9].strip('"').replace("00", "").lstrip("0") or "0"
    price_2 = row[10].strip('"').replace("00", "").lstrip("0") or "0"

    # Build the final string, semicolon-delimited
    return f"{code} ; {name} ; {value_1} ; {value_2} ; {price} ; {tf_value}"


def transform_huvudgrupp(row):
    """
    Transform huvudgrupp rows.
    """

    # Safety check: make sure we have enough columns
    if len(row) < 7:
        return ""

    # Determine T or F
    tf_value = "False" if row[0].strip('"') == "03" else "True"

    # Extract columns (strip() to remove accidental whitespace)
    huvudgrupp_code = row[4].strip('"')
    name = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{tf_value} ; {huvudgrupp_code} ; {name}"


def transform_varugrupp(row):
    """Transform varugrupp rows."""

    # Safety check: make sure we have enough columns
    if len(row) < 7:
        return ""

    # Determine T or F
    tf_value = "False" if row[0].strip('"') == "03" else "True"

    # Extract columns (strip() to remove accidental whitespace)
    huvudgrupp_code = row[4].strip('"')
    varugrupp_code = row[5].strip('"')
    name = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{tf_value} ; {huvudgrupp_code} ; {varugrupp_code} ; {name}"
