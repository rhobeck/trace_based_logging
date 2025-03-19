from web3 import Web3
import time
from src.trace_based_logging.logging_config import setup_logging


logger = setup_logging()


def get_txIndex(hash_list, node_url):
    #path = os.path.join(dir_path, "resources", 'df_trace_tree_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.csv')
    #hashes = pd.read_csv(path, usecols=["hash"])

    # hashes.reset_index(inplace=True, drop=True)
    # hash_list = hashes["hash"].unique()

#    dir_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))

#    path = os.path.join(dir_path, "config.json")

#    with open(path, 'r') as file:
#        config = json.load(file)
#    port = config["port"]
#    protocol = config["protocol"]
#    host = config["host"]
#    node_url = protocol + host + ":" + str(port)
    w3 = Web3(Web3.HTTPProvider(node_url))


    # Check connection
    if not w3.isConnected():
        logger.info("Query transaction index: Failed to connect to Ethereum node")
        exit()


    # Dictionary to store transaction indexes
    transaction_indexes = {}

    # Maximum number of retries for a failed transaction query
    max_retries = 15
    # Delay between retries in seconds
    retry_delay = 2

    for index, tx_hash in enumerate(hash_list, start=1):
        retries = 0
        while retries < max_retries:
            try:
                # Retrieve the transaction by hash
                transaction = w3.eth.get_transaction(tx_hash)
                # Store the transaction index in the dictionary
                transaction_indexes[tx_hash] = transaction['transactionIndex']
                logger.info(f"Query transaction index: Processed transaction {index} out of {len(hash_list)}: {tx_hash}")
                break  # Exit the retry loop on success
            except Exception as e:
                logger.info(f"Query transaction index: Error retrieving transaction {tx_hash}: {e}. Retrying... {retries + 1}/{max_retries}")
                retries += 1
                time.sleep(retry_delay)
        
        if retries == max_retries:
            logger.info(f"Query transaction index: Failed to retrieve transaction {tx_hash} after {max_retries} retries.")

    #path = os.path.join(dir_path, "resources", 'transaction_indexes_' + base_contract + "_" + str(min_block) + "_" + str(max_block) + '.pkl')
    #pickle.dump(transaction_indexes, open(path, "wb"))
    return transaction_indexes