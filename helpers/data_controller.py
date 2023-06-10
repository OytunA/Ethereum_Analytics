import requests
import json

def fetch_blocks_data(first_block, last_block, payload, url):
    # Fetch data for each block in the range
    data = []
    for block_number in range(first_block, last_block):
        payload['params'][0] = hex(block_number)  # Convert block number to hexadecimal
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            block_data = json.loads(response.text)
            data.append(block_data['result'])
    return data

def convert_to_float(x):
    if isinstance(x, float):
        return x
    elif isinstance(x, str):
        value = int(x, base=16)
        return float(value)  # Convert to float
    else:
        return None

def get_eth_logs(payload, url):
    # Send the JSON-RPC request
    response = requests.post(url, json=payload)

    # Parse the response
    data = response.json()

    return data
