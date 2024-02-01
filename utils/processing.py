import logging
import multiprocessing as mp
import os
import pandas as pd
import numpy as np

from utils.loading import TICKER_PATHS, \
    BUYER_PATH, \
    SELLER_PATH
from utils.preprocessing import preprocess_ticker

logging.basicConfig(level=logging.INFO)


def process_all_tickers(ticker_paths=TICKER_PATHS,
                        cpu_count=mp.cpu_count()-1):
    """
    Process all tickers in parallel.

    Parameters
    ----------
    ticker_paths : list of str
        List of paths to tickers.
    cpu_count : int
        Number of cores to use.

    Returns
    -------
    df_buyer_stats : pd.DataFrame
        Dataframe containing buyer stats.
    """
    cpu_count = min(cpu_count, mp.cpu_count()-1)
    with mp.Pool(cpu_count) as pool:
        pool.map(process_and_save_ticker, ticker_paths)


def process_and_save_ticker(ticker_path,
                            buyer_path=BUYER_PATH,
                            seller_path=SELLER_PATH):
    """
    Process and save a ticker.

    Parameters
    ----------
    ticker_path : str
        Path to ticker.
    buyer_path : str
        Path to save buyer stats.
        default: BUYER_PATH
    seller_path : str
        Path to save seller stats.
        default: SELLER_PATH
    """
    try:
        df_buyer, df_seller = buyer_seller_stats(ticker_path)

        buyer_filename = os.path.join(buyer_path,
                                      os.path.basename(ticker_path))\
            + '.parquet'
        seller_filename = os.path.join(seller_path,
                                       os.path.basename(ticker_path))\
            + '.parquet'

        df_buyer.to_parquet(buyer_filename)
        df_seller.to_parquet(seller_filename)
        # logging.info(f'Processed and saved {ticker_path}...')

    except Exception:
        logging.error(f'Error processing {ticker_path}...')


def buyer_seller_stats(file_name: str):
    """
    Calculate buyer and seller stats for a given ticker file
    and return the results as two dataframes.

    Parameters
    ----------
    file_name : str
        Path to ticker.

    Returns
    -------
    df_buyer_stats : pd.DataFrame
        Dataframe containing buyer stats.
    df_seller_stats : pd.DataFrame
        Dataframe containing seller stats.
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
    """
    Calculate trader stats for a given ticker file
    and return the results as a dataframe.

    Parameters
    ----------
    df_trade_stats : pd.DataFrame
        Dataframe containing ticker data.
    trader_type : str
        Type of trader to calculate stats for.
        default: 'buyer'

    Returns
    -------
    df_trader_stats : pd.DataFrame
        Dataframe containing trader stats.
    """

    trader_col = f'{trader_type}_id'

    df_day_stats = df_trade_stats[['day',
                                   'day_std',
                                   'day_volume']]\
        .drop_duplicates().\
        copy()

    df_trade_stats = df_trade_stats[['day',
                                     'datetime',
                                     'trade-price',
                                     'trade-volume',
                                     trader_col]].copy()

    df_trader_stats = df_trade_stats.groupby(['day', trader_col])\
        .agg(
          start_price=('trade-price', 'first'),
          end_price=('trade-price', 'last'),
          total_volume=('trade-volume', 'sum'),
          metaorder_start=('datetime', 'min'),
          metaorder_end=('datetime', 'max')
    ).reset_index()
    df_trader_stats['metaorder_duration'] = df_trader_stats['metaorder_end']\
        - df_trader_stats['metaorder_start']
    df_trader_stats['metaorder_duration'] = \
        df_trader_stats['metaorder_duration'].dt.total_seconds()

    df_trader_stats = pd.merge(df_trader_stats, df_day_stats, on='day')

    df_trader_stats['price_impact'] = (df_trader_stats['end_price'] -
                                       df_trader_stats['start_price']).abs()
    df_trader_stats['price_impact_pct'] = df_trader_stats['price_impact']
    df_trader_stats['volume_pct'] = df_trader_stats['total_volume']\
        / df_trader_stats['day_volume']

    return df_trader_stats[['volume_pct',
                            'price_impact_pct',
                            'metaorder_duration',
                            'day_std']]


def daily_volume_std(df: pd.DataFrame):
    """
    Calculate daily volume standard deviation for a given ticker file
    and return the results as a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing ticker data.

    Returns
    -------
    df_daily_vol_std : pd.DataFrame
        Dataframe containing daily volume standard deviation.
    """

    df_daily_vol_std = df.groupby(df['datetime'].dt.date)\
        .agg({
            'trade-volume': 'sum',
            'trade-price': 'std'
            })\
        .reset_index()\
        .rename(columns={
            'datetime': 'day',
            'trade-volume': 'day_volume',
            'trade-price': 'day_price_std'
            })

    df_daily_vol_std['day'] = pd.to_datetime(df_daily_vol_std['day'])

    return df_daily_vol_std


def daily_vol(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily volatility for a given ticker file
    and return the results as a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing ticker data.

    Returns
    -------
    df : pd.DataFrame
        Dataframe containing daily volatility.
    """

    df = df.copy()
    df.sort_values('datetime', inplace=True)
    # df = df.groupby('day')['trade-price']\
    #     .apply(lambda x: np.log(x / x.shift(1)))\
    #     .reset_index()\
    #     .groupby('day')['trade-price']\
    #        .std()\
    #        .reset_index()
    # df = df.groupby('day')['trade-price']\
    #     .std()\
    #     .reset_index()
    df['log_return'] = df['trade-price'].pct_change().apply(lambda x:
                                                            np.log(1+x))

    daily_volatility = df['log_return'].groupby(df['datetime'].dt.date)\
        .std()\
        .reset_index(name='day_std')\
        .rename(columns={'datetime': 'day'})
    daily_volatility['day'] = pd.to_datetime(daily_volatility['day'])
    return daily_volatility


def daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily volume for a given ticker file
    and return the results as a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing ticker data.

    Returns
    -------
    df : pd.DataFrame
        Dataframe containing daily volume.
    """

    df = df.copy()
    df.sort_values('datetime', inplace=True)
    df = df.groupby('day')['trade-volume']\
           .sum()\
           .reset_index(). \
        rename(columns={'trade-volume': 'day_volume'})
    df['day'] = pd.to_datetime(df['day'])

    return df
