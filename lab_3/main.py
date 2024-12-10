import csv
import json
import re

from checksum import serialize_result, calculate_checksum

VAR = 76
CSV_PATH = f"{VAR}.csv"
PATTERNS = "patterns.json"


def read_patterns(path_to_file: str) -> dict:
    """
    A function for reading patterns from a JSON file and returning a dictionary.

    Parameters
        path: the path to the JSON file to read

    Returns
        Dictionary of patterns from a JSON file
    """
    try:
        with open(path_to_file, 'r', encoding='UTF-8') as fp:
            patterns = json.load(fp)
        return patterns
    except FileNotFoundError:
        print(f"The file '{path_to_file}' was not found")
    except Exception as e:
        print(f"An error occurred while reading the JSON file: {str(e)}")


def read_csv(file_path: str) -> list:
    """
    Reads data from a csv file and returns a list of rows as nested lists.

    Parameters
        file_path: The path to the csv file.

    Returns
        A list of lists, each containing the values of a row in the csv file.
    """
    data = []
    try:
        with open(file_path, "r", encoding="utf16") as csv_file:
            reader = csv.reader(csv_file, delimiter=";")
            for row in reader:
                data.append(row)
            data.pop(0)
    except Exception as e:
        print(f"Error while reading {file_path}: {e}")
    return data


def check_valid_line(row: list, patterns: dict) -> bool:
    """
    Checks the validity of a data row against specified patterns.

    Parameters
        row: A list representing a row of data to be validated.
        patterns: A dictionary where keys are column names and values are regex
        patterns for validation.

    Returns
        bool: True if the row is valid according to the patterns, False otherwise.
    """
    for key, value in zip(patterns.keys(), row):
        if not re.match(patterns[key], value):
            return False
    return True


def verify_data(path_csv: str, path_json: str) -> list:
    """
    Returns a list of row numbers that do not match the regular expressions.

    Parameters
        path_csv: The path to the dataset file.
        path_json: The path to the file with regular expressions.

    Returns
        list: A list of invalid row numbers.
    """
    invalid_rows = []
    data = read_csv(path_csv)
    regexps = read_patterns(path_json)
    for i, row in enumerate(data):
        if not check_valid_line(row, regexps):
            invalid_rows.append(i)
    return invalid_rows


if __name__ == "__main__":
    invalid = verify_data(CSV_PATH, PATTERNS)
    hash_sum = calculate_checksum(invalid)
    serialize_result(VAR, hash_sum)