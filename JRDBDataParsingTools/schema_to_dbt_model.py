import sys
import yaml


def translate_yaml(input_yaml):
    # Load the input YAML
    data = yaml.safe_load(input_yaml)

    # Modify the data according to the new format
    translated_data = []
    for item in data:
        translated_item = {"name": item["name"]}
        if item["comments"]:
            translated_item["description"] = item["comments"]
        translated_data.append(translated_item)

    # Convert the modified data back to YAML
    return yaml.dump(translated_data, allow_unicode=True, sort_keys=False)


if __name__ == "__main__":
    # Check if the YAML file path argument is provided
    if len(sys.argv) < 2:
        print("Please provide the YAML file path as an argument.")
        sys.exit(1)

    # Read the YAML file
    file_path = sys.argv[1]
    with open(file_path, "r") as file:
        input_yaml = file.read()

    # Translate the YAML and print the result
    translated_yaml = translate_yaml(input_yaml)
    print(translated_yaml)
