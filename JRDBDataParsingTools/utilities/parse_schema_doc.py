import sys
import yaml


def parse_schema(schema_text):
    """
    Parses the schema text and returns a list of dictionaries containing the field details.
    :param schema_text: The schema text to be parsed
    :return: A list of dictionaries containing the field details

    This is a best effort parser and does not work for all schema documents.
    It is just for getting a head start on the schema parsing, instead of having to type everything manually.
    """
    # Splitting the schema text into lines
    lines = schema_text.split("\n")

    # Markers to identify the start and end of the schema fields
    marker = "*" * 79  # Assuming the marker is 79 asterisks based on the example
    marker_count = 0

    schema_list = []
    current_group = ""

    for line in lines:
        if marker in line:
            marker_count += 1
            continue

        # Processing lines only between the second and third occurrence of the marker
        if 2 <= marker_count < 3:
            parts = line.strip().split()
            if len(parts) < 4:
                # Assuming lines with less than 4 parts are group names
                if parts and parts[0].startswith("===") | parts[0].startswith("（"):
                    current_group = ""
                elif parts:
                    current_group = parts[0]
                else:
                    current_group = ""
            elif len(parts) >= 4 and (parts[1].isdigit() or parts[1].isalpha()):
                # Lines with 4 or more parts are considered fields
                field_name = (
                    f"{current_group}_{parts[0]}" if current_group else parts[0]
                )

                # Creating a dictionary for each field with its details
                field_entry = {
                    "name": field_name,
                    # "occ": parts[1],
                    "byte_length": int(parts[1]),
                    "format": parts[2],
                    "relative": parts[3] if len(parts) > 3 else "",
                    "comments": " ".join(parts[4:]) if len(parts) > 4 else "",
                }
                schema_list.append(field_entry)

    return schema_list


# Checking if the file path is provided as a command line argument
if len(sys.argv) < 2:
    print("Please provide the file path as a command line argument.")
    sys.exit(1)

# Reading the file contents
file_path = sys.argv[1]
with open(file_path, "r") as file:
    file_contents = file.read()

# Parsing the schema
schema = parse_schema(file_contents)

# Converting the parsed schema to YAML string
yaml_output = yaml.dump(schema, allow_unicode=True, sort_keys=False)

# Printing the YAML output
print(yaml_output)
