import requests
import pandas as pd
from pandasql import sqldf

from config import json_filename_for_ethereum_logs, payload_of_ethgetlogs
from helpers.data_controller import convert_to_float
from helpers.json_file_controller import save_data_to_json, read_json_data
from helpers.queries import top_transfer_events_query, top_addresses_received_most_token


def task_3_worker():

    # Task 3.1
    url = "https://eth.llamarpc.com"
    response = requests.post(url, json=payload_of_ethgetlogs)

    data = response.json()

    save_data_to_json(data, json_filename_for_ethereum_logs)



    # Task 3.2
    # Read the JSON data
    json_data = read_json_data(json_filename_for_ethereum_logs)

    # Convert JSON data to a DataFrame
    result_df = pd.json_normalize(json_data, record_path='result')

    result_df['topics'] = result_df['topics'].apply(lambda x: x[0] if len(x) > 0 else None)
    # Execute the query and get the result as a DataFrame
    top_erc_df = sqldf(top_transfer_events_query, locals())

    print("Top 10 ERC-20 Tokens with the Highest Number of Transfer Events:")
    print(top_erc_df)

    # Task 3.3
    result_df['data'] = result_df['data'].apply(lambda x: convert_to_float(x))
    top_address_df = sqldf(top_addresses_received_most_token, locals())

    print("Top 10 Addresses that have Received the most Tokens:")
    print(top_address_df)

if __name__ == "__main__":
    print("Task3 started.")
    task_3_worker()
    print("Task3 finished.")
