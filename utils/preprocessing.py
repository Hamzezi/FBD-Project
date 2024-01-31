import pandas as pd
import re

from datetime import datetime, \
    timedelta
from utils.loading import read_ticker


def preprocess_ticker(file_name):
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

    df_cleaned['day'] = df_cleaned.datetime.dt.date
    df_cleaned['day'] = pd.to_datetime(df_cleaned['day'])

    return df_cleaned


def xltime_to_datetime(xltime):
    excel_epoch = datetime(1899, 12, 30)
    return excel_epoch + timedelta(days=xltime)


def extract_ids(s, id_type):
    pattern = rf"\[{id_type}\](\d+)"
    match = re.search(pattern, s)
    return int(match.group(1)) if match else None
