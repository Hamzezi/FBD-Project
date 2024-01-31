import logging
import numpy as np
import os
import pandas as pd

from utils.loading import BUYER_PATH, SELLER_PATH
from utils.preprocessing import preprocess_ticker

logging.basicConfig(level=logging.INFO)


def process_and_save_tiker(ticker_path):
    try:
        df_buyer, df_seller = buyer_seller_stats(ticker_path)

        buyer_filename = os.path.join(BUYER_PATH,
                                      os.path.basename(ticker_path))\
            + '.parquet'
        seller_filename = os.path.join(SELLER_PATH,
                                       os.path.basename(ticker_path))\
            + '.parquet'

        df_buyer.to_parquet(buyer_filename)
        df_seller.to_parquet(seller_filename)
        logging.info(f'Processed and saved {ticker_path}...')

    except ValueError:
        logging.info(f'No data for {ticker_path}...')


def buyer_seller_stats(file_name: str):
    """
    Calculate buyer and seller stats for a given ticker file
    :param file_name: str
    :return: pd.DataFrame, pd.DataFrame: buyer stats, seller stats P
    """
    df = preprocess_ticker(file_name)   
    df_daily_volume = daily_volume(df)
    df_daily_vol = daily_vol(df)

    df_daily_vol_std = pd.merge(df_daily_volume, df_daily_vol, on='day')

    df_trade_stats = pd.merge(df, df_daily_vol_std, on='day')

    df_buyer_stats = trader_stats(df_trade_stats, trader_type='buyer')
    df_seller_stats = trader_stats(df_trade_stats, trader_type='seller')

    df_buyer_stats.dropna(inplace=True)
    df_seller_stats.dropna(inplace=True)

    return df_buyer_stats, df_seller_stats


def trader_stats(df_trade_stats: pd.DataFrame,
                 trader_type: str = 'buyer') -> pd.DataFrame:

    trader_col = f'{trader_type}_id'

    df_day_stats = df_trade_stats[['day', 'day_std', 'day_volume']]\
        .drop_duplicates().\
        copy()

    df_trade_stats = df_trade_stats[['day',
                                     'trade-price',
                                     'trade-volume',
                                     trader_col]].copy()
    df_trader_stats = df_trade_stats.groupby(['day', trader_col])\
        .agg(
          start_price=('trade-price', 'first'),
          end_price=('trade-price', 'last'),
          total_volume=('trade-volume', 'sum')
    ).reset_index()
    df_trader_stats = pd.merge(df_trader_stats, df_day_stats, on='day')

    df_trader_stats['price_impact'] = (df_trader_stats['end_price'] - 
                                       df_trader_stats['start_price']).abs()
    df_trader_stats['price_impact_pct'] = df_trader_stats['price_impact']\
        / df_trader_stats['day_std']
    df_trader_stats['volume_pct'] = df_trader_stats['total_volume']\
        / df_trader_stats['day_volume']

    return df_trader_stats[['volume_pct', 'price_impact_pct']]


def daily_volume_std(df: pd.DataFrame):
    df_daily_vol_std = df.groupby(df['datetime'].dt.date)\
     .agg({'trade-volume': 'sum',
           'trade-price': 'std'})\
     .reset_index()\
     .rename(columns={'datetime': 'day',
                      'trade-volume': 'day_volume',
                      'trade-price': 'day_price_std'})

    df_daily_vol_std['day'] = pd.to_datetime(df_daily_vol_std['day'])

    return df_daily_vol_std


def daily_vol(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_values('datetime', inplace=True)
    df = df.groupby('day')['trade-price']\
        .apply(lambda x: np.log(x / x.shift(1)))\
        .reset_index()\
        .groupby('day')['trade-price']\
           .std()\
           .reset_index()

    df['day'] = pd.to_datetime(df['day'])
    df.rename(columns={'trade-price': 'day_std'}, inplace=True)
    return df


def daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_values('datetime', inplace=True)
    df = df.groupby('day')['trade-volume']\
           .sum()\
           .reset_index(). \
        rename(columns={'trade-volume': 'day_volume'})
    df['day'] = pd.to_datetime(df['day'])

    return df
