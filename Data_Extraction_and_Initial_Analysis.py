from config import first_block, last_block, payload_of_eth_getblockbynumber, url, json_filename_for_ethereum_data
from helpers.data_controller import fetch_blocks_data
from helpers.json_file_controller import save_data_to_json, read_json_data
import pandas as pd

def task_1_worker():

    #Task 1.1
    ethereum_data = fetch_blocks_data(first_block, last_block, payload_of_eth_getblockbynumber, url)
    save_data_to_json(ethereum_data, json_filename_for_ethereum_data)

    #Task 1.2
    # Read the JSON data
    json_data = read_json_data(json_filename_for_ethereum_data)

    # Convert JSON data to a DataFrame
    transactions_df = pd.json_normalize(json_data, record_path='transactions')

    # Perform exploratory data analysis
    average_gas_price = transactions_df['gasPrice'].apply(lambda x: int(x, 16)).mean()
    total_ether_transferred = transactions_df['value'].apply(lambda x: int(x, 16)).sum()

    print(f"Average Gas Price: {average_gas_price}")
    print(f"Total Ether Transferred: {total_ether_transferred}")


if __name__ == "__main__":
    print("Task1 started.")
    task_1_worker()
    print("Task1 finished.")


