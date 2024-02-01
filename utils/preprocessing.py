import pandas as pd
import re

from datetime import datetime, \
    timedelta
from utils.loading import read_ticker


def preprocess_ticker(file_name):
    """
    Preprocess a ticker file.

    Parameters
    ----------
    file_name : str
        Name of file to read.

    Returns
    -------
    df_cleaned : pd.DataFrame
        Dataframe containing ticker data.
    """

    df = read_ticker(file_name)

    if df.empty:
        raise ValueError(f"Empty dataframe for {file_name}")

    df['datetime'] = df['xltime'].apply(xltime_to_datetime)

    df['seller_id'] = df['trade-rawflag']\
        .apply(lambda x: extract_ids(x, 'BNPP Seller ID'))
    df['buyer_id'] = df['trade-rawflag']\
        .apply(lambda x: extract_ids(x, 'BNPP Buyer ID'))

    df['trade-stringflag'] = df['trade-stringflag'].astype('category')

    df_cleaned = df[['datetime',
                     'trade-price',
                     'trade-volume',
                     'seller_id',
                     'buyer_id']].copy()
    # Remove trades with same seller and buyer
    df_cleaned = df_cleaned[df_cleaned['seller_id'] != df_cleaned['buyer_id']]
    df_cleaned['day'] = df_cleaned.datetime.dt.date
    df_cleaned['day'] = pd.to_datetime(df_cleaned['day'])

    return df_cleaned


def xltime_to_datetime(xltime):
    """
    Convert Excel time to datetime.

    Parameters
    ----------
    xltime : float
        Excel time.

    Returns
    -------
    datetime : datetime
        Datetime object.
    """
    excel_epoch = datetime(1899, 12, 30)
    return excel_epoch + timedelta(days=xltime)


def extract_ids(s, id_type):
    """
    Extract trader IDs from a string.

    Parameters
    ----------
    s : str
        String to extract IDs from. Example:
        '[BNPP Seller ID]1234[BNPP Buyer ID]5678'
    id_type : str
        Type of ID to extract. Example:
        'BNPP Seller ID'

    Returns
    -------
    id : int
        Trader ID.

        Example:

        1234 if id_type is 'BNPP Seller ID' and s is '[BNPP Seller ID]1234'
    """
    pattern = rf"\[{id_type}\](\d+)"
    match = re.search(pattern, s)
    return int(match.group(1)) if match else None
