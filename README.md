# Logging Ethereum Applications Based on Transaction Traces

This project is designed log execution data of decentralized applications (DApps) deployed to Ethereum.
The execution data is generated by recomputing transaction traces (i.e., recreating the computational steps transaction execution).
On a high level, the code implements the following functionality for a user-defined blockrange: 

1. Re-create transaction traces for contract addresses.
2. Discover addresses belonging to a DApp by following CREATE relations from contract deployments in the transaction trace data.
3. Decode the blockchain events and function call input data in the transaction traces based on contract ABIs.
4. Example code to transform the decoded trace data to be ready for creating an event log for object-centric process mining.

There are two noteworthy prerequisites for running the program: 
- Etherscan API Key. The transaction retrieval is based querying normal transactions and internal transactions from Etherscan.  
- Ethereum Archive Node with a Geth-based client (e.g., Geth or Erigon).
  - Transaction retrieval by events (Ethereum log entries) uses the Python web3 library function ```eth.get_logs``` https://web3py.readthedocs.io/en/stable/web3.eth.html#web3.eth.Eth.get_logs 
  - The program recreates transaction traces based on the Geth debug function ```debug_traceTransaction``` https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_traceTransaction


## Installation

1. Clone the repository:
```console
git clone https://github.com/rhobeck/trace_based_logging
```

2. Install the required dependencies:
```console
pip install -r requirements.txt
```


## Configuration

Before running the main script, configure the json files in the project folder. 

`config.json`, a file with basic parameters:
- `port`: Port to connect to the Ethereum archive node.
- `protocol`: Protocol to transfer information.
- `host`: IP address to connect to the Ethereum archive node.
- `list_contracts_lx`: List of contract addresses that belong to the DApp.
- `list_predefined_non_dapp_contracts`: List of contract addresses that you do not wish to be included into the list of DApp contracts.
- `min_block`: Minimum block number for the data query.
- `max_block`: Maximum block number for the data query.
- `extract_normal_transactions`: Flag to set to `true` if normal transactions shall be queried from Etherscan.
- `extract_internal_transactions`: Flag to set to `true` if internal transactions shall be queried from Etherscan.
- `extract_transactions_by_events`: Flag to set to `true` if blockchain events shall be queried from the Ethereum node.
- `etherscan_api_key`: Your Etherscan API key for fetching contract ABIs.
- `dapp_decode_events`: Flag set `true` if events emitted by contracts of the DApp shall be decoded.
- `dapp_decode_calls_with_ether_transfer`: Flag set `true` if calls with Ether transfers to contracts of the DApp shall be decoded.
- `dapp_decode_calls_with_no_ether_transfer`: Flag set `true` if calls with no Ether transfers to contracts of the DApp shall be decoded.
- `dapp_decode_delegatecalls`: Flag set `true` if delegatecalls to contracts of the DApp shall be decoded.
- `non_dapp_decode_events`: Flag set `true` if events emitted by contracts not belonging to the DApp shall be decoded.
- `non_dapp_decode_calls_with_ether_transfer`: Flag set `true` if calls with Ether transfer to contracts not belonging to the DApp shall be decoded.
- `non_dapp_decode_calls_with_no_ether_transfer`: Flag set `true` if calls with no Ether transfer to  contracts not belonging to the DApp shall be decoded.
- `non_dapp_decode_delegatecalls`: Flag set `true` if delegatecalls to contracts not belonging to the DApp shall be decoded.

`config_custom_events.json`, a file with ABI specifications for decoding the data. Reason: even if the ABI of a contract is not available (on Etherscan), event or functions specified in `config_custom_events.json` can still be decoded. By default this includes ABI specifications to decode the events from the standards ERC-20, ERC-721, ERC-777, and ERC-1155 (and two custom Augur events).


## Usage

To execute the main function of the project, run:
```console
python main.py
```

## Related publications

- Hobeck, R., Weber, I. (2023). *Towards Object-Centric Process Mining for Blockchain Applications*. In: Köpke, J., et al. Business Process Management: Blockchain, Robotic Process Automation and Educators Forum. BPM 2023. Lecture Notes in Business Information Processing, vol 491. Springer, Cham. DOI: [10.1007/978-3-031-43433-4_4](https://doi.org/10.1007/978-3-031-43433-4_4)


## Remarks

Please note that docstrings in this repository were drafted by ChatGPT and edited by the author.


## License

This project is distributed under the [MIT license](LICENSE).