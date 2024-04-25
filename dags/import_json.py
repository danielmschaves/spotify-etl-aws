import json

# Load the JSON data from the file
with open('data/raw/playlist_37i9dQZEVXbMDoHDwVN2tF_si=e8e1e56d145e4f9b_20.json', 'r', encoding='utf-8') as file:
    json_data = json.load(file)

# Check if the data is a list and print the structure of the first item if it is
if isinstance(json_data, list) and json_data:
    # Print the keys and a snippet of the first item to understand its structure
    print(json_data[0].keys())
    print(json.dumps(json_data[0], indent=4, ensure_ascii=False))  # Prints a formatted string of the first item
else:
    print("The JSON does not contain a list or is empty.")