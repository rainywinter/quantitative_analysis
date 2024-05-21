"""
股价日线、年线等之间的转换
"""

import pandas as pd


def day2period(df=pd.DataFrame, sample="d"):
    df.index = pd.to_datetime(df["date"], format="%Y-%m-%d")

    to_df = pd.DataFrame()
    to_df["open"] = df["open"].resample(sample).first()
    to_df["close"] = df["close"].resample(sample).last()
    to_df["high"] = df["high"].resample(sample).max()
    to_df["low"] = df["low"].resample(sample).min()
    # to_df.reset_index(drop=True, inplace=True)

    df.reset_index(drop=True, inplace=True)
    return to_df


def day2week(df=pd.DataFrame):
    return day2period(df, sample="W")


def day2month(df=pd.DataFrame):
    return day2period(df, sample="ME")


def day2year(df=pd.DataFrame):
    return day2period(df, sample="Y")
