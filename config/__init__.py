first_block = 17000000
last_block = 17005000
url = "https://rpc.ankr.com/eth"
payload_of_eth_getblockbynumber = {
    'jsonrpc': '2.0',
    'method': 'eth_getBlockByNumber',
    'params': ['0x', True],  # '0x' is a placeholder for the block number
    'id': 1
}
json_filename_for_ethereum_data = "ethereum_data.json"
url_for_logs = "https://eth.llamarpc.com"
payload_of_ethgetlogs = {
    'jsonrpc': '2.0',
    'method': 'eth_getLogs',
    'params': [{
        'fromBlock': hex(first_block),
        'toBlock': hex(last_block),
        'topics': []
    }],
    'id': 1
}
json_filename_for_ethereum_logs = "ethereum_logs.json"
