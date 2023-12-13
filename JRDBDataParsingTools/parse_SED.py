import os
import pandas as pd
import yaml
import argparse
from pydantic import BaseModel, DirectoryPath


class Arguments(BaseModel):
    schema_file_path: str
    data_directory_path: DirectoryPath
    output_directory_path: DirectoryPath


# Function to parse a line based on the schema
def parse_line(line, schema):
    parsed_data = {}
    for field in schema:
        start = field.relative - 1  # Adjust for zero-based indexing
        end = start + field.byte_length
        parsed_data[field.name] = line[start:end].decode("cp932").strip()
    return parsed_data


def parse_file(file_path, schema):
    with open(file_path, "rb") as file:
        for line in file:
            return parse_line(line, schema)


def main(args: Arguments):
    # load the schema from the provided file path
    with open(args.schema_file_path, "r") as file:
        schema = yaml.safe_load(file)

    # Read the data files in the directory and parse them
    parsed_records = []
    for filename in os.listdir(args.data_directory_path):
        file_path = os.path.join(args.data_directory_path, filename)
        parsed_records.append(parse_file(file_path, schema))

    # Convert to a DataFrame for better visualization
    df = pd.DataFrame(parsed_records)
    print(df.head())  # Displaying the first few records for review

    # Export each file to the output directory
    for i, filename in enumerate(os.listdir(args.data_directory_path)):
        file_path = os.path.join(args.output_directory_path, f"output_{i}.csv")
        df.to_csv(file_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("schema_file_path", help="Path to the schema file")
    parser.add_argument("data_directory_path", help="Path to the data directory")
    parser.add_argument("output_directory_path", help="Path to the output directory")
    args = Arguments(**vars(parser.parse_args()))
    main(args)
