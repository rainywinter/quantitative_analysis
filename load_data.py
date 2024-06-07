"""
加载本地数据
"""

from datetime import datetime
from enum import Enum
import os
import time

import pandas as pd

import log
import config as cfg
import tdx_mapping
import xline


def load_a_shares():
    """读取股票代码-股票名称文件
    返回DataFrame
    columns: ['code', 'name']
    """
    df = pd.read_csv(
        cfg.a_share_path, sep=",", encoding="utf-8", converters={"code": str}
    )

    return df


def a_shares_to_dict(df=pd.DataFrame):
    """
    将df转换为name-code，code-name的2个dict
    """
    dt_codes, dt_names = dict(), dict()

    for index, row in df.iterrows():
        dt_codes[row["code"]] = row["name"]
        dt_names[row["name"]] = row["code"]
    return dt_codes, dt_names


class ReportPeriod(Enum):
    """
    财报周期枚举
    """

    # 一季报
    QUQRTER = "0331"
    # 中报
    SIME = "0630"
    # 三季报
    THIRD = "0930"
    # 年报
    ANNUAL = "1231"


def load_a_annual_finance_reports(period=ReportPeriod.ANNUAL, rename_column=True):
    """
    加载A股全部年报
    """
    log.i(f"加载财报:begin, period={period}, rename_column={rename_column}")
    start = time.time()

    df = pd.DataFrame()

    fnames = [
        name
        for name in os.listdir(cfg.ProcessedDataPath.tdx_cw)
        if period.value in name
    ]
    for name in fnames:
        path = cfg.ProcessedDataPath.tdx_cw + os.sep + name
        if os.path.getsize(path) == 0:
            continue
        tmp_df = pd.read_pickle(path, compression=None)
        tmp_df["date"] = datetime.strptime(name[4:-4], "%Y%m%d")
        df = pd.concat([df, tmp_df])

    if rename_column:
        df.rename(columns=tdx_mapping.finance_mapping, inplace=True)

    df = df.sort_values(by=["code", "date"], ascending=True)
    log.i(f"加载财报:end,共{df.shape[0]}份 用时{(time.time() - start):.2f}秒")
    return df


def load_gbbq():
    """
    加载股本变迁csv
    return DataFrame
    """
    df = pd.read_csv(
        cfg.ProcessedDataPath.tdx_gbbq, encoding="utf-8", dtype={"code": str}
    )
    df["date"] = pd.to_datetime(df["权息日"], format="%Y%m%d")
    return df


# def load_a_lday(code="000423", date_to_datetime=False):
#     """
#     加载某股票全部日线数据(前复权)
#     返回dataframe
#     """
#     df = pd.read_csv(
#         cfg.ProcessedDataPath.tdx_lday_qfq + os.sep + share + ".csv",
#         index_col=None,
#         encoding="gbk",
#         dtype={"code": str},
#     )
#     if date_to_datetime:
#         df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
#     return df


def load_line_day(code="000001"):
    """加载股票日线"""
    path = cfg.ProcessedDataPath.tdx_lday_qfq + os.sep + code + ".csv"
    if not os.path.exists(path):
        print(f"code={code} 日线数据文件不存在")
        return pd.DataFrame()

    df = pd.read_csv(
        path,
        index_col=None,
        encoding="gbk",
        dtype={"code": str},
    )

    return df


""" 全局变量 """

# dataframe  A股股票代码、名称
df_a_shares = None
# dict 股票{code:name},{name:code}
dt_a_share_codes, dt_a_share_names = None, None
# 财报
df_a_share_annual_finance_reports = None


def init_global():

    global df_a_shares
    if df_a_shares is None:
        df_a_shares = load_a_shares()

    global dt_a_share_codes, dt_a_share_names
    if dt_a_share_codes is None:
        dt_a_share_codes, dt_a_share_names = a_shares_to_dict(df_a_shares)

    global df_a_share_annual_finance_reports
    if df_a_share_annual_finance_reports is None:
        df_a_share_annual_finance_reports = load_a_annual_finance_reports()


if __name__ == "__main__":
    print()
