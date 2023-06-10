import pandas as pd
from pandasql import sqldf

from config import json_filename_for_ethereum_data
from helpers.data_controller import convert_to_float
from helpers.json_file_controller import read_json_data
from helpers.queries import top_addresses_query, top_contracts_query


def task_2_worker():

    # Read the JSON data
    json_data = read_json_data(json_filename_for_ethereum_data)

    # Convert JSON data to a DataFrame
    transactions_df = pd.json_normalize(json_data, record_path='transactions')

    # Take specified columns
    sub_transactions_df = transactions_df.loc[:, ['value', 'blockNumber', 'to']]

    # Apply the base-16 conversion to specific columns in the DataFrame
    columns_that_convert_to_float = ['value', 'blockNumber']
    columns_that_convert_to_str = ['to']

    sub_transactions_df[columns_that_convert_to_float] = sub_transactions_df[columns_that_convert_to_float].applymap(convert_to_float)
    sub_transactions_df[columns_that_convert_to_str] = sub_transactions_df[columns_that_convert_to_str].astype(str)

    # Task2.1
    # Execute the query and get the result as a DataFrame
    top_addresses_df = sqldf(top_addresses_query, locals())

    # Task2.2
    # Execute the query and get the result as a DataFrame
    top_contracts_df = sqldf(top_contracts_query, locals())

    print("Top 10 Ethereum Addresses by Total Ether Received:")
    print(top_addresses_df)

    print("Top 5 Smart Contracts by Total Number of Transactions:")
    print(top_contracts_df)

    # Task2.3
    # Convert JSON data to a DataFrame
    result_df = pd.json_normalize(json_data)

    # Apply the base-16 conversion to specific columns in the DataFrame
    result_df['timestamp'] = result_df['timestamp'].apply(lambda x: int(x, 16))

    # Convert timestamp to datetime
    result_df['timestamp'] = pd.to_datetime(result_df['timestamp'], unit='s')

    # Set timestamp as the index
    result_df.set_index('timestamp', inplace=True)

    # Extract 'gasPrice' from 'transactions' column and convert to float
    result_df['gasPrice'] = result_df['transactions'].apply(lambda x: convert_to_float(x) if x and x[0]['gasPrice'] else None)

    # Resample data on an hourly basis
    hourly_data = result_df.resample('H').agg({
        'gasPrice': 'mean',
        'transactions': 'size'
    })

    # Identify unusual spikes in gas prices and transaction volumes
    gas_price_spike_threshold = 1.5 * hourly_data['gasPrice'].std()
    transaction_volume_spike_threshold = 1.5 * hourly_data['transactions'].std()

    gas_price_spikes = hourly_data[hourly_data['gasPrice'] > gas_price_spike_threshold]
    transaction_volume_spikes = hourly_data[hourly_data['transactions'] > transaction_volume_spike_threshold]

    print("Gas Price Spikes:")
    print(gas_price_spikes)

    print("Transaction Volume Spikes:")
    print(transaction_volume_spikes)
    print(result_df['gasPrice'])

if __name__ == "__main__":
    print("Task2 started.")
    task_2_worker()
    print("Task2 finished.")