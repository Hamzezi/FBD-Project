import glob
import os
import pandas as pd
import shutil
import tarfile

from pathlib import Path

UTILS_PATH = Path(__file__).parent.absolute()
MAIN_DIR_PATH = UTILS_PATH.parent
DATA_DIR_PATH = MAIN_DIR_PATH / 'data'
TAR_FILE_PATH = DATA_DIR_PATH / 'ES_fut_chain.tar'
EXTRACT_PATH = DATA_DIR_PATH / 'extracts'

CA_TRADE_PATH = os.path.join(DATA_DIR_PATH, 'trade')

TICKER_PATHS = [os.path.join(CA_TRADE_PATH, ticker)
                for ticker in os.listdir(CA_TRADE_PATH)]
SAVE_PATH = os.path.join(DATA_DIR_PATH, 'processed')
BUYER_PATH = os.path.join(SAVE_PATH, 'buyer')
SELLER_PATH = os.path.join(SAVE_PATH, 'seller')

if not os.path.exists(SAVE_PATH):
    os.makedirs(SAVE_PATH)
if not os.path.exists(BUYER_PATH):
    os.makedirs(BUYER_PATH)
if not os.path.exists(SELLER_PATH):
    os.makedirs(SELLER_PATH)


def read_ticker(folder_path):
    """
    Read all files in a folder and return a dataframe.

    Parameters
    ----------
    folder_path : str
        Path to folder containing ticker files, either parquet or csv.

    Returns
    -------
    df : pd.DataFrame
        Dataframe containing ticker data.
    """
    # check if folder contains parquet files or csv files
    all_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    read_func = pd.read_parquet
    if len(all_files) == 0:
        all_files = glob.glob(os.path.join(folder_path, "*.csv.gz"))
        read_func = pd.read_csv

    # read all files in folder
    df = pd.concat((read_func(f) for f in all_files))

    return df


def read_data_from_tar(file_name,
                       tar_path=TAR_FILE_PATH,
                       extract_path=EXTRACT_PATH):
    """
    Read a file from a tar archive and return a dataframe.

    Parameters
    ----------
    file_name : str
        Name of file to read.
    tar_path : str
        Path to tar archive.
        default: TAR_FILE_PATH
    extract_path : str
        Path to extract tar archive.
        default: EXTRACT_PATH

    Returns
    -------
    df : pd.DataFrame
        Dataframe containing ticker data.
    """

    with tarfile.open(tar_path, 'r') as tar:
        tar.extract(file_name, path=extract_path)
    df = pd.read_parquet(os.path.join(extract_path, file_name))
    return df


def dir_empty(dir_path):
    """
    Check if a directory is empty.

    Parameters
    ----------
    dir_path : str
        Path to directory.

    Returns
    -------
    bool
        True if directory is empty, False otherwise.
    """
    return len(os.listdir(dir_path)) == 0


def clean_extracts(extract_path=EXTRACT_PATH):
    """
    Delete all files in a directory.

    Parameters
    ----------
    extract_path : str
        Path to directory.
        default: EXTRACT_PATH
    """
    shutil.rmtree(extract_path)
