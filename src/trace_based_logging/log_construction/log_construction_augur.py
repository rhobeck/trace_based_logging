import os
import pandas as pd
import pyarrow.parquet as pq
import sys
import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from src.trace_based_logging.logging_config import setup_logging
from pm4py.objects.ocel.obj import OCEL
from pm4py.objects.ocel.exporter.xmlocel.exporter import apply as export_ocel
from pm4py.objects.ocel.exporter.xmlocel.exporter import Variants

logger = setup_logging()


def fix_dataframe_for_parquet(df):
    """
    For columns with dtype object that contain non-bytes values (e.g. ints, floats, arrays),
    convert them to strings to avoid pyarrow conversion errors.
    """
    for col in df.columns:
        if df[col].dtype == object:
            nonnull = df[col].dropna()
            if not nonnull.empty:
                sample = nonnull.iloc[0]
                # If sample is not bytes, convert entire column to string.
                df[col] = df[col].apply(lambda x: str(x) if (not np.isscalar(x)) or (pd.notnull(x)) else x)
                logger.info(f"Converted column '{col}' to string.")
    return df


def convert_to_parquet(resources_dir, log_folder, dataset_name, base_contract, min_block, max_block):
    """
    Converts a dataset (saved as CSV/PKL) to Parquet.
    The function builds the filename using the same scheme as during saving:
        file_base = f"{dataset_name}_{base_contract}_{min_block}_{max_block}"
    
    Args:
        resources_dir (str): Base directory where log_folder is located.
        log_folder (str): Folder name where the files are stored.
        dataset_name (str): Base name of the dataset (without extension).
        base_contract (str): Base contract address used in the file naming.
        min_block (int): Minimum block number.
        max_block (int): Maximum block number.
    """
    file_base = f"{dataset_name}_{base_contract}_{min_block}_{max_block}"
    pkl_path = os.path.join(resources_dir, log_folder, file_base + ".pkl")
    csv_path = os.path.join(resources_dir, log_folder, file_base + ".csv")
    
    if os.path.exists(pkl_path):
        df = pd.read_pickle(pkl_path)
        logger.info(f"Loaded {file_base} from PKL.")
    elif os.path.exists(csv_path):
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {file_base} from CSV.")
    else:
        logger.error(f"Neither PKL nor CSV file found for {file_base}. Skipping conversion.")
        return

    # Fix the dataframe to avoid pyarrow conversion issues.
    df = fix_dataframe_for_parquet(df)

    parquet_path = os.path.join(resources_dir, log_folder, file_base + ".parquet")
    try:
        df.to_parquet(parquet_path)
        logger.info(f"Converted {file_base} to Parquet: {parquet_path}")
    except Exception as e:
        logger.error(f"Failed to write {file_base} to Parquet: {e}")


def convert_datasets(resources_dir, log_folder, toggles, toggle_to_filename, base_contract, min_block, max_block):
    """
    Converts all datasets marked as True in the toggles dictionary to Parquet,
    using the same file naming scheme as in the save function.
    
    Args:
        resources_dir (str): Base resources directory.
        log_folder (str): Folder containing the datasets.
        toggles (dict): Dictionary controlling which datasets to process.
        toggle_to_filename (dict): Mapping of toggle keys to dataset base names.
        base_contract (str): Base contract address.
        min_block (int): Minimum block number.
        max_block (int): Maximum block number.
        
    Returns:
        list: A list of dataset base names that were selected for conversion.
    """
    selected_datasets = [
        toggle_to_filename[key] for key, value in toggles.items()
        if value and key in toggle_to_filename
    ]
    logger.info(f"Datasets selected for conversion: {selected_datasets}")
    
    for ds in selected_datasets:
        convert_to_parquet(resources_dir, log_folder, ds, base_contract, min_block, max_block)
    
    return selected_datasets


def load_overall_dataframe(resources_dir, log_folder, selected_datasets, desired_columns, base_contract, min_block, max_block):
    """
    Loads the converted parquet files for the selected datasets and concatenates them into a single DataFrame.
    The function uses the same file naming convention as during saving.
    
    Args:
        resources_dir (str): Base directory of the resources.
        log_folder (str): Folder where the parquet files are stored.
        selected_datasets (list): List of dataset base names to load.
        desired_columns (set): Set of columns desired to be loaded.
        base_contract (str): Base contract address.
        min_block (int): Minimum block number.
        max_block (int): Maximum block number.
    
    Returns:
        pd.DataFrame: A concatenated DataFrame of all loaded parquet files.
    """
    overall_dataframe_list = []
    
    for ds in selected_datasets:
        file_base = f"{ds}_{base_contract}_{min_block}_{max_block}"
        parquet_path = os.path.join(resources_dir, log_folder, file_base + ".parquet")
        if os.path.exists(parquet_path):
            try:
                parquet_file = pq.ParquetFile(parquet_path)
                column_names = set(parquet_file.schema.names)
                columns_to_load = sorted(list(column_names.intersection(desired_columns)))
                df = pd.read_parquet(parquet_path, columns=columns_to_load)
                df.dropna(how='all', axis=1, inplace=True)
                overall_dataframe_list.append(df)
                logger.info(f"Loaded parquet file {parquet_path} with columns: {columns_to_load}")
            except Exception as e:
                logger.error(f"Error loading parquet file {parquet_path}: {e}")
        else:
            logger.error(f"Parquet file not found for dataset: {file_base}")
    
    if overall_dataframe_list:
        overall_dataframe = pd.concat(overall_dataframe_list)
        logger.info(f"Combined overall dataframe shape: {overall_dataframe.shape}")
    else:
        overall_dataframe = pd.DataFrame()
        logger.error("No dataframes were loaded to combine.")
    
    return overall_dataframe


def create_ocel_files(overall_dataframe, output_dir):
    """
    This function was contributed by Alessandro Berti.
    
    Creates and stores the OCEL objects, events, and event-to-object relations (e2o)
    as Parquet files in the output directory using the schema provided.
    Finally, it builds an OCEL object using pm4py.
    
    Args:
        overall_dataframe (pd.DataFrame): The combined dataframe with all event data.
        output_dir (str): Directory where the OCEL Parquet files will be stored.
        
    Returns:
        OCEL: An OCEL object built from the stored Parquet files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created OCEL output directory: {output_dir}")

    # Construct objects
    objects_list = []

    # EOA objects: union of unique senders and targets.
    eoa = sorted(list(set(overall_dataframe.dropna(subset=["sender"])["sender"].unique()).union(set(overall_dataframe.dropna(subset=["target"])["target"].unique()))))
    eoa = pd.DataFrame({"ocel:oid": eoa})
    eoa["ocel:oid"] = "EOA_" + eoa["ocel:oid"]
    eoa["ocel:type"] = "EOA"
    eoa["category"] = "EOA"
    objects_list.append(eoa)

    # Tokens
    tokens = overall_dataframe.dropna(subset=["token"]).groupby("token")["tokenType_name"].first().reset_index()
    tokens.rename(columns={"token": "ocel:oid", "tokenType_name": "category"}, inplace=True)
    tokens["ocel:oid"] = "TOKEN_" + tokens["ocel:oid"]
    tokens["ocel:type"] = "TOKEN"
    tokens["category"] = tokens["category"].astype('string')
    objects_list.append(tokens)

    # Orders
    orders = overall_dataframe.dropna(subset=["orderId"]).groupby("orderId")["orderType"].first().reset_index()
    orders.rename(columns={"orderId": "ocel:oid", "orderType": "category"}, inplace=True)
    orders["ocel:oid"] = "ORDER_" + orders["ocel:oid"]
    orders["ocel:type"] = "ORDER"
    orders["category"] = orders["category"].astype('string')
    objects_list.append(orders)

    # Contracts
    contracts = overall_dataframe.dropna(subset=["address"]).groupby("address")["contract_name"].first().reset_index()
    contracts.rename(columns={"address": "ocel:oid", "contract_name": "category"}, inplace=True)
    contracts["ocel:oid"] = "CONTRACT_" + contracts["ocel:oid"]
    contracts["ocel:type"] = "CONTRACT"
    contracts["category"] = contracts["category"].astype('string')
    objects_list.append(contracts)

    # Markets
    markets = overall_dataframe.dropna(subset=["market"]).groupby("market")["marketType"].first().reset_index()
    markets.rename(columns={"market": "ocel:oid", "marketType": "category"}, inplace=True)
    markets["ocel:oid"] = "MARKET_" + markets["ocel:oid"]
    markets["ocel:type"] = "MARKET"
    markets["category"] = markets["category"].astype('string')
    objects_list.append(markets)

    objects_df = pd.concat(objects_list)
    del eoa
    del tokens
    del orders
    del contracts
    del markets
    logger.info(f"Constructed objects dataframe with shape: {objects_df.shape}")

    # Prepare overall_dataframe for events and e2o.
    overall_dataframe.sort_values(["timeStamp", "tracePos"], inplace=True)
    overall_dataframe.rename(columns={"Activity": "ocel:activity", "timeStamp": "ocel:timestamp"}, inplace=True)
    overall_dataframe["ocel:eid"] = "EID_" + overall_dataframe.index.astype(str)

    # Build event-to-object relations
    e2o_list = []

    eoa_from = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "sender"]].dropna()
    eoa_from = eoa_from.rename(columns={"sender": "ocel:oid"})
    eoa_from["ocel:oid"] = "EOA_" + eoa_from["ocel:oid"]
    eoa_from["ocel:type"] = "EOA"
    eoa_from["ocel:qualifier"] = "EOA_FROM"
    e2o_list.append(eoa_from)

    eoa_to = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "target"]].dropna()
    eoa_to = eoa_to.rename(columns={"target": "ocel:oid"})
    eoa_to["ocel:oid"] = "EOA_" + eoa_to["ocel:oid"]
    eoa_to["ocel:type"] = "EOA"
    eoa_to["ocel:qualifier"] = "EOA_TO"
    e2o_list.append(eoa_to)

    tokens_e2o = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "token"]].dropna()
    tokens_e2o = tokens_e2o.rename(columns={"token": "ocel:oid"})
    tokens_e2o["ocel:oid"] = "TOKEN_" + tokens_e2o["ocel:oid"]
    tokens_e2o["ocel:type"] = "TOKEN"
    tokens_e2o["ocel:qualifier"] = "TOKEN"
    e2o_list.append(tokens_e2o)

    orders_e2o = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "orderId"]].dropna()
    orders_e2o = orders_e2o.rename(columns={"orderId": "ocel:oid"})
    orders_e2o["ocel:oid"] = "ORDER_" + orders_e2o["ocel:oid"]
    orders_e2o["ocel:type"] = "ORDER"
    orders_e2o["ocel:qualifier"] = "ORDER"
    e2o_list.append(orders_e2o)

    contracts_e2o = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "address"]].dropna()
    contracts_e2o = contracts_e2o.rename(columns={"address": "ocel:oid"})
    contracts_e2o["ocel:oid"] = "CONTRACT_" + contracts_e2o["ocel:oid"]
    contracts_e2o["ocel:type"] = "CONTRACT"
    contracts_e2o["ocel:qualifier"] = "CONTRACT"
    e2o_list.append(contracts_e2o)

    markets_e2o = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp", "market"]].dropna()
    markets_e2o = markets_e2o.rename(columns={"market": "ocel:oid"})
    markets_e2o["ocel:oid"] = "MARKET_" + markets_e2o["ocel:oid"]
    markets_e2o["ocel:type"] = "MARKET"
    markets_e2o["ocel:qualifier"] = "MARKET"
    e2o_list.append(markets_e2o)

    e2o_df = pd.concat(e2o_list)
    del eoa_from
    del eoa_to
    del tokens_e2o
    del orders_e2o
    del contracts_e2o
    del markets_e2o
    del e2o_list
    
    e2o_df.sort_values(["ocel:timestamp", "tracePos", "ocel:oid"], inplace=True)
    if "tracePos" in e2o_df.columns:
        del e2o_df["tracePos"]
    #e2o_df = e2o_df.groupby(["ocel:eid", "ocel:oid"]).first().reset_index()
    logger.info(f"Constructed event-to-object relations dataframe with shape: {e2o_df.shape}")

    events_df = overall_dataframe[["tracePos", "ocel:eid", "ocel:activity", "ocel:timestamp"]].dropna()
    events_df.sort_values(["ocel:timestamp", "tracePos"], inplace=True)
    if "tracePos" in events_df.columns:
        del events_df["tracePos"]
    logger.info(f"Constructed events dataframe with shape: {events_df.shape}")

    # Write OCEL components to parquet files.
    objects_path = os.path.join(output_dir, "ocel_objects.parquet")
    events_path = os.path.join(output_dir, "ocel_events.parquet")
    e2o_path = os.path.join(output_dir, "ocel_e2o.parquet")

    objects_df.to_parquet(objects_path, index=False)
    logger.info(f"OCEL objects written to: {objects_path}")
    events_df.to_parquet(events_path, index=False)
    logger.info(f"OCEL events written to: {events_path}")
    e2o_df.to_parquet(e2o_path, index=False)
    logger.info(f"OCEL event-to-object relations written to: {e2o_path}")

    # Build the OCEL object using pm4py.
    ocel = OCEL(
        events=pd.read_parquet(events_path),
        objects=pd.read_parquet(objects_path),
        relations=pd.read_parquet(e2o_path)
    )
    
    
    
    logger.info("OCEL object created successfully.")
    return ocel

def save_ocel_xml(ocel, resources_dir, log_folder, base_contract, min_block, max_block):
    """
    Saves the OCEL object as an XML-OCEL file using the CLASSIC variant.
    The file is saved using the same naming scheme as the saved CSV/PKL files.
    
    Args:
        ocel (OCEL): The OCEL object.
        resources_dir (str): Base directory.
        log_folder (str): Log folder name.
        base_contract (str): Base contract address.
        min_block (int): Minimum block number.
        max_block (int): Maximum block number.
    """
    file_base = f"ocel_{base_contract}_{min_block}_{max_block}.xmlocel"
    target_path = os.path.join(resources_dir, log_folder, file_base)
    try:
        export_ocel(ocel, target_path, variant=Variants.CLASSIC)
        logger.info(f"OCEL exported to XML-OCEL at: {target_path}")
    except Exception as e:
        logger.error(f"Error exporting OCEL to XML-OCEL: {e}")


if __name__ == '__main__':
    RESOURCES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'resources'))
    LOG_FOLDER = "log_120325"
    CONFIG = {
        "base_contract": "0x75228dce4d82566d93068a8d5d49435216551599",
        "min_block": 5926229,
        "max_block": 11229577
    }
    
    # Define toggles (only these affect dataset conversion and loading)
    toggles = {
        "events_dapp": True,
        "calls_dapp": True,
        "delegatecalls_dapp": False,
        "zero_value_calls_dapp": False,
        "creations_dapp": False,
        "events_non_dapp": False,
        "calls_non_dapp": False,
        "delegatecalls_non_dapp": False,
        "zero_value_calls_non_dapp": False,
        "zero_value_delegatecalls_non_dapp": False,
        "creations_non_dapp": False,
        "sensitive_events": False
    }
    
    # Mapping toggle keys to corresponding dataset base filenames
    toggle_to_filename = {
        "events_dapp": "events_dapp",
        "calls_dapp": "calls_dapp",
        "delegatecalls_dapp": "delegatecalls_dapp",
        "zero_value_calls_dapp": "calls_dapp_zero_value",
        "creations_dapp": "creations_dapp",
        "events_non_dapp": "events_non_dapp",
        "calls_non_dapp": "calls_non_dapp",
        "delegatecalls_non_dapp": "delegatecalls_non_dapp",
        "zero_value_calls_non_dapp": "calls_non_dapp_zero_value",
        "zero_value_delegatecalls_non_dapp": "delegatecalls_non_dapp_zero_value",
        "creations_non_dapp": "creations_non_dapp"
    }
    
    # Convert selected datasets to Parquet using the same filename scheme as when saving.
    selected_datasets = convert_datasets(
        RESOURCES_DIR,
        LOG_FOLDER,
        toggles,
        toggle_to_filename,
        CONFIG["base_contract"],
        CONFIG["min_block"],
        CONFIG["max_block"]
    )
    
    # Define desired columns for loading the parquet files.
    desired_columns = {"timeStamp", "Activity", "tracePos", "sender", "target", 
                       "market", "marketType", "address", "token", "contract_name", 
                       "tokenType_name", "orderId", "orderType"}
    
    # Load and concatenate the overall dataframe from the converted parquet files.
    overall_dataframe = load_overall_dataframe(
        RESOURCES_DIR,
        LOG_FOLDER,
        selected_datasets,
        desired_columns,
        CONFIG["base_contract"],
        CONFIG["min_block"],
        CONFIG["max_block"]
    )
     
    # Create OCEL files and object.
    ocel_obj = create_ocel_files(overall_dataframe, RESOURCES_DIR)
    
    # Save the OCEL object as an XML-OCEL file using the same naming scheme.
    save_ocel_xml(ocel_obj, RESOURCES_DIR, LOG_FOLDER, CONFIG["base_contract"], CONFIG["min_block"], CONFIG["max_block"])
    
    logger.info("Data processing complete.")
