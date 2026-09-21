import os
import pytz
import csv
from datetime import datetime

from logger_setup import get_logger

logger = get_logger(__name__)


def import_action(file_paths):
    """
    Takes a list of file paths, expects exactly one 'pcs.adm' file,
    and splits its contents into four new files based on the rules:

    1) '01' or '11'  --> first output file
    2) '02' or '22'  --> second output file
    3) '03' or '33'  --> check 6th column:
       - if empty    --> third output file
       - if not empty -> fourth output file

    The caller (main.py) is responsible for waiting until the source file has
    finished being written before calling this - see wait_for_file_stable.
    """
    if not file_paths:
        logger.warning("No file paths provided to import_action.")
        return

    pcs_file_path = file_paths[0]
    if not os.path.exists(pcs_file_path):
        logger.error(f"File does not exist: {pcs_file_path}")
        return

    # Define output file names in the same directory as pcs_file_path
    #base_dir = os.path.dirname(pcs_file_path)
    import_folder = os.path.join("C:\\", "winbag_export", "Imported_Files")

    if not os.path.exists(import_folder):
        os.makedirs(import_folder)
        logger.info(f"Created 'Imported Files' folder at {import_folder}")

    logger.debug(f"Resolved import folder path: {os.path.abspath(import_folder)}")


    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    output1_path = os.path.join(import_folder, f"file_01_11.{current_time}.csv")
    output2_path = os.path.join(import_folder, f"file_artiklar.{current_time}.csv")
    output3_path = os.path.join(import_folder, f"file_huvudgrupp.{current_time}.csv")
    output4_path = os.path.join(import_folder, f"file_varugrupp.{current_time}.csv")

    # Write to .tmp paths and only rename to the real names once processing
    # finishes without error. If the process gets interrupted partway through
    # (crash, kill, restart), this leaves either nothing or an obviously
    # incomplete .tmp file instead of a silently-truncated "real" output file.
    final_paths = [output1_path, output2_path, output3_path, output4_path]
    tmp_paths = [p + ".tmp" for p in final_paths]

    row_counts = {"file_01_11": 0, "file_artiklar": 0, "file_huvudgrupp": 0, "file_varugrupp": 0}
    skipped_rows = 0
    unrecognized_codes = {}

    try:
        with open(pcs_file_path, "r", encoding="cp1252") as pcs_in, open(
            tmp_paths[0], "w", encoding="cp1252"
        ) as out1, open(tmp_paths[1], "w", encoding="cp1252") as out2, open(
            tmp_paths[2], "w", encoding="cp1252"
        ) as out3, open(
            tmp_paths[3], "w", encoding="cp1252"
        ) as out4:

            for line_number, line in enumerate(pcs_in, start=1):
                # Remove trailing newline/spaces
                clean_line = line.strip()
                if not clean_line:
                    continue  # skip empty lines

                try:
                    reader = csv.reader([clean_line], delimiter=",", quotechar='"')
                    row = next(reader)
                except (csv.Error, StopIteration):
                    logger.warning(
                        f"Skipping malformed CSV line {line_number} in {pcs_file_path}: {clean_line!r}"
                    )
                    skipped_rows += 1
                    continue

                # We need at least one column to proceed
                if not row:
                    continue

                first_value = row[0].strip('"')  # remove leading/trailing quotes

                # 01 / 11 --> file 1
                if first_value in ("01", "11"):
                    tranformed_row = transform_01_11(row, line_number)
                    if tranformed_row:
                        out1.write(tranformed_row + "\n")
                        row_counts["file_01_11"] += 1
                    else:
                        skipped_rows += 1

                # 02 / 22 --> file 2
                elif first_value in ("02", "22"):
                    tranformed_row = transform_02_22(row, line_number)
                    if tranformed_row:
                        out2.write(tranformed_row + "\n")
                        row_counts["file_artiklar"] += 1
                    else:
                        skipped_rows += 1

                # 03 / 33 --> check the 6th column
                elif first_value in ("03", "33"):
                    if len(row) >= 6 and row[5].strip('"'):
                        # There's a value in the 6th column
                        transformed_row = transform_varugrupp(row, line_number)
                        if transformed_row:
                            out4.write(transformed_row + "\n")
                            row_counts["file_varugrupp"] += 1
                        else:
                            skipped_rows += 1
                    else:
                        # The 6th column is empty or doesn't exist
                        transformed_row = transform_huvudgrupp(row, line_number)
                        if transformed_row:
                            out3.write(transformed_row + "\n")
                            row_counts["file_huvudgrupp"] += 1
                        else:
                            skipped_rows += 1

                elif first_value in ("00", "99"):
                    pass

                else:
                    unrecognized_codes[first_value] = unrecognized_codes.get(first_value, 0) + 1

        # Only now that every line has been processed without error do the
        # output files get their real names - see comment above on tmp_paths.
        for tmp_path, final_path in zip(tmp_paths, final_paths):
            os.replace(tmp_path, final_path)

        logger.info(
            f"import_action completed successfully for {pcs_file_path}. "
            f"Rows written: {row_counts}. Skipped/malformed rows: {skipped_rows}."
        )
        if unrecognized_codes:
            logger.warning(
                f"Encountered unrecognized row codes in {pcs_file_path}: {unrecognized_codes}"
            )

    except Exception:
        logger.exception(f"An error occurred in import_action for {pcs_file_path}")
        for tmp_path in tmp_paths:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                logger.warning(f"Could not remove incomplete temp file {tmp_path}")


def transform_01_11(row, line_number=None):
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
        logger.warning(
            f"Skipping 01/11 row at line {line_number}: expected at least 7 columns, got {len(row)}: {row}"
        )
        return ""

    # print(f"First value: {row[0]}")

    # Determine T or F
    tf_value = "false" if row[0].strip('"') == "01" else "true"

    # Extract columns (strip() to remove accidental whitespace)
    code = row[3].strip('"')
    name = row[4].strip('"')
    address = row[5].strip('"')
    desc = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{tf_value};{code};{name};{address};{desc}"


def transform_02_22(row, line_number=None):
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

    mapping = {
        "2500": "1",
        "1200": "2",
        "0600": "3",
    }

    # Safety check: make sure we have enough columns
    if len(row) < 21:
        logger.warning(
            f"Skipping 02/22 row at line {line_number}: expected at least 21 columns, got {len(row)}: {row}"
        )
        return ""

    # Determine T or F
    descat = "false" if row[0].strip('"') == "02" else "true"

    # Extract columns (strip() to remove accidental whitespace)
    item_ref = row[3].strip('"')
    item_name = row[4].strip('"')
    department_id = row[6].strip('"')
    section_id = row[7].strip('"')

    sale_price_1_raw = row[8].strip('"').strip()
    # Convert to integer, divide by 100, or default to "0" if empty/invalid
    try:
        sale_price_1 = f"{int(sale_price_1_raw) / 100:.2f}".replace(".", ",")
    except (ValueError, TypeError):
        logger.warning(
            f"Line {line_number}: could not parse sale_price_1 {sale_price_1_raw!r} "
            f"for item {item_ref} ({item_name}). Defaulting to 0,00."
        )
        sale_price_1 = "0,00"

    vat_1 = mapping.get(row[9].strip('"'), "0")
    price_list_code_1 = 1

    sale_price_2_raw = row[10].strip('"').strip()
    try:
        sale_price_2 = f"{int(sale_price_2_raw) / 100:.2f}".replace(".", ",")
    except (ValueError, TypeError):
        logger.warning(
            f"Line {line_number}: could not parse sale_price_2 {sale_price_2_raw!r} "
            f"for item {item_ref} ({item_name}). Defaulting to 0,00."
        )
        sale_price_2 = "0,00"

    vat_2 = mapping.get(row[11].strip('"'), "0")
    price_list_code_2 = 2

    # Build the final string, semicolon-delimited
    return f"{item_ref};{item_name};{department_id};{section_id};{sale_price_1};{vat_1};{price_list_code_1};{sale_price_2};{vat_2};{price_list_code_2};{descat}"


def transform_huvudgrupp(row, line_number=None):
    """
    Transform huvudgrupp rows.
    """

    # Safety check: make sure we have enough columns
    if len(row) < 7:
        logger.warning(
            f"Skipping huvudgrupp row at line {line_number}: expected at least 7 columns, got {len(row)}: {row}"
        )
        return ""

    # Determine T or F
    tf_value = "false" if row[0].strip('"') == "03" else "true"

    # Extract columns (strip() to remove accidental whitespace)
    huvudgrupp_code = row[4].strip('"')
    name = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{huvudgrupp_code};{name}"


def transform_varugrupp(row, line_number=None):
    """Transform varugrupp rows."""

    # Safety check: make sure we have enough columns
    if len(row) < 7:
        logger.warning(
            f"Skipping varugrupp row at line {line_number}: expected at least 7 columns, got {len(row)}: {row}"
        )
        return ""

    # Determine T or F
    tf_value = "false" if row[0].strip('"') == "03" else "true"

    # Extract columns (strip() to remove accidental whitespace)
    huvudgrupp_code = row[4].strip('"')
    varugrupp_code = row[5].strip('"')
    name = row[6].strip('"')

    # Build the final string, semicolon-delimited
    return f"{huvudgrupp_code};{varugrupp_code};{name}"
