top_addresses_query = """
SELECT "to", SUM(value) as total_received
FROM sub_transactions_df
WHERE blockNumber BETWEEN 17000000 AND 17005000
GROUP BY "to"
ORDER BY total_received DESC
LIMIT 10
"""

top_contracts_query = """
SELECT blockNumber, COUNT(*) as transaction_count
FROM sub_transactions_df
WHERE blockNumber BETWEEN 17000000 AND 17005000
GROUP BY blockNumber
ORDER BY transaction_count DESC
LIMIT 5
"""

top_transfer_events_query = """
SELECT address, COUNT(*) AS transfer_count
FROM result_df
WHERE topics LIKE '0x%'
GROUP BY address
ORDER BY transfer_count DESC
LIMIT 10
"""

top_addresses_received_most_token = """
SELECT address, SUM(data) AS total_received
FROM result_df
GROUP BY to_address
ORDER BY total_received DESC
LIMIT 10;
"""