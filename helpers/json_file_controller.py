import json

def save_data_to_json(data, json_filename):
    # Save the data in JSON format
    with open(json_filename, 'w') as file:
        json.dump(data, file)
    return f"{json_filename} is created."

def read_json_data(json_filename):
    with open(json_filename, 'r') as file:
        data = json.load(file)
    return data