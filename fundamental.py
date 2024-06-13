"""
基本面分析
"""

import os, sys, math
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib import dates
from tqdm import tqdm
import numpy as np

import config as cfg
import load_data
import xline


def core_indicator_calc(df=pd.DataFrame):
    """
    核心指标计算
    """
    # 单位 万元
    for c in [
        "信用减值损失",
        "信用减值损失2018",
        "信用减值损失2019",
        "合同负债",
        "应收款项融资",
    ]:
        df[c] = df[c] * 10000

    df["核心利润"] = (
        df["营业收入"] - df["营业成本"] - df["销售费用"] - df["管理费"] - df["财务费"]
    )

    df["核心利润获现率"] = 10 * (df["经营活动产生的现金流量净额"] / df["核心利润"])
    df["应收款项"] = (
        df["应收票据"] + df["应收账款"] + df["长期应收款"] + df["应收款项融资"]
    )

    # 战略发展的支撑 指标
    df["有息负债"] = (
        df["短期借款"]
        + df["一年内到期的非流动负债"]
        + df["交易性金融负债"]
        + df["长期借款"]
        + df["应付债券"]
    )
    df["经营性负债"] = (
        df["应付票据"]
        + df["应付账款"]
        + df["长期应付款"]
        + df["预收款项"]
        + df["合同负债"]
    )
    df["股东入资"] = df["股本"] + df["资本公积"]
    df["利润积累"] = df["盈余公积"] + df["未分配利润"]

    return df


def core_indicator_plot(code="000001", to_web=False, df_gbbq=pd.DataFrame):
    """
    核心指标plot
    to_web=True 返回figure对象，不plot。false时直接plot
    df_gbbq: 股本变迁dataframe
    """
    # 加载日线并转换为月线
    df_price = xline.day2month(load_data.load_line_day(code))

    # 财报数据
    df = df_core_indicator[df_core_indicator["code"] == code].sort_values(
        by="date", ascending=True
    )

    if df_gbbq is None or df_gbbq.empty:
        df_gbbq = load_data.load_gbbq()

    # 分红数据
    df_gbbq.rename(columns={"分红-前流通盘": "dividend"}, inplace=True)
    df_dividend = df_gbbq[(df_gbbq["code"] == code) & (df_gbbq["类别"] == "除权除息")]
    df_dividend.loc[:, ["year"]] = pd.DatetimeIndex(df_dividend["date"]).year - 1
    df_dividend = df_dividend[["year", "dividend"]]
    # groupby 后自动设置index
    df_dividend = df_dividend.groupby("year").sum() / 10

    df["year"] = pd.DatetimeIndex(df["date"]).year
    df.set_index("year", drop=True, inplace=True)

    df = pd.concat([df, df_dividend], axis=1)
    df.fillna({"dividend": 0}, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["dividend"] = df["dividend"] * 10
    df.rename(columns={"dividend": "每股分红*10"}, inplace=True)
    df["基本每股收益*10"] = df["基本每股收益"] * 10

    # 处理噪点异常值，消除图像尖点
    cols = [
        "核心利润获现率",
        "净资产收益率",
        "销售毛利率",
        "销售净利率",
        "营业收入增长率",
        "净利润增长率",
    ]
    desc = df[cols].describe()
    for c in cols:
        df.loc[df[c] > 2 * desc.loc["75%", c], c] = 2 * desc.loc["75%", c]
        min = desc.loc["25%", c]
        if desc.loc["min", c] < 0 and min > 0:
            min = -10
        df.loc[df[c] < 0, c] = min

    if to_web:
        fig = Figure(figsize=(20, 28))
        cols = 2  # web上显示2列
    else:
        fig = plt.figure(figsize=(18, 10))
        cols = 3  # plot显示3列
    name = load_data.dt_a_share_codes[code]
    fig.suptitle(code + " " + name)

    indicators = [
        "股价走势",
        "营业收入",
        ["净利润", "经营活动产生的现金流量净额", "核心利润", "应收款项"],
        [
            "应付票据",
            "应付账款",
            "预付款项",
        ],  # 付款
        [
            "预收款项",
            "合同负债",
            "应收账款",
            "应收款项融资",
            "应收票据",
        ],  # 收款
        ["每股净资产", "每股分红*10", "基本每股收益*10"],
        ["信用减值损失", "信用减值损失2018", "信用减值损失2019", "商誉"],
        "核心利润获现率",
        # ["存货周转率", "固定资产周转率", "总资产周转率"],
        [
            "净资产收益率",
            "销售毛利率",
            "销售净利率",
            "财务费用率",
        ],
        # ["营业收入增长率", "净利润增长率"],
        ["有息负债", "经营性负债", "股东入资", "利润积累"],
    ]
    rows = math.ceil(len(indicators) / cols)
    count = 1
    marker = "."
    for i in indicators:
        ax = fig.add_subplot(rows, cols, count)
        if type(i) == list:
            # ax.set_title("-".join(i))
            ax.set_title(i[0])
            ax.plot(df["date"], df[i], marker=marker)
            ax.legend(i, loc="best")
        else:
            ax.set_title(i)
            if i == "股价走势":
                ax.plot(df_price[["close"]], marker=marker)
            else:
                ax.plot(df["date"], df[[i]], marker=marker)
            ax.legend([i], loc="best")
        count = count + 1

    if to_web:
        return fig

    plt.subplots_adjust(wspace=0.3, hspace=0.3)
    plt.show()
    return


def calc_avg_book_value_grow():
    """
    计算上市以来净资产增长
    """
    # 2010年来
    begin = "2010-12-31"
    df_finance = df_core_indicator[df_core_indicator["date"] >= begin]
    df_group = df_finance.groupby("code")
    # 2019年6月注册制 pass
    milestone = pd.Timestamp("2019-06-01")

    # 股本变迁， 计算分红用
    df_gbbq = load_data.load_gbbq()

    df_shares = load_data.df_a_shares.copy()
    df_shares = df_shares.dropna()
    df_shares.set_index("code", drop=True, inplace=True)

    data = []
    for code, df in tqdm(df_group):
        # index 可能为[0 0 0...]，需要重置
        df.reset_index(drop=True, inplace=True)

        if code not in df_shares.index:
            ok = False
        else:
            ok = df_shares.loc[code, "listing_date"] < "2019-06-01"
        if not ok:
            # print(code, df.loc[0, "date"])
            continue

        date = df.loc[0, "date"]

        year = df.shape[0]
        begin, end = df.loc[0, "每股净资产"], df.loc[df.index[-1], "每股净资产"]

        if year == 0 or begin <= 0 or end <= 0:
            # print("negtive book value:", code, year, end, begin)
            continue
        grow = end / begin
        rate = math.pow(grow, 1 / year)

        # 股本变迁中分红单位为10股
        dividend = (
            df_gbbq[(df_gbbq["code"] == code) & (df_gbbq["类别"] == "除权除息")][
                "分红-前流通盘"
            ].sum()
            / 10
        )
        obj = {
            "date": date,
            "name": load_data.dt_a_share_codes[code],
            "code": code,
            "start_book_value": begin,
            "end_book_value": end,
            "total_dividend": dividend,
            "grow": grow,
            "rate": rate,
        }
        # print(f"obj:{obj}")
        data.append(obj)
        # df_grow = pd.concat([df_grow, pd.DataFrame([obj])], axis=0)

    df_grow = pd.DataFrame.from_dict(data)
    df_grow.sort_values(by="rate", ascending=False, inplace=True)
    df_grow.to_csv(
        cfg.TdxCfg.root_path + os.sep + "book_rate_grow.csv",
        encoding="utf-8",
        index=False,
    )
    print(df_grow)


# 全部股票核心指标dataframe
df_core_indicator = None


def init_global():
    global df_core_indicator
    if df_core_indicator is None:
        df_core_indicator = core_indicator_calc(
            load_data.df_a_share_annual_finance_reports
        )


if __name__ == "__main__":
    load_data.init_global()
    init_global()

    df_gbbq = load_data.load_gbbq()

    # calc_avg_book_value_grow()
    # raise Exception("test")

    codes = [name[:-4] for name in os.listdir(cfg.ProcessedDataPath.tdx_lday_qfq)]
    codes.sort()
    index = codes.index(load_data.dt_a_share_names["伊利股份"])

    record = False
    if len(sys.argv) > 1 and sys.argv[1] == "lastest":
        with open(".cache/lastest", "r") as f:
            line = f.read()
            print("lastest code", line)
            index = codes.index(line)
            record = True

    codes = codes[index:]
    df = df_core_indicator
    for c in codes:
        core_indicator_plot(code=c, to_web=False, df_gbbq=df_gbbq)
        if record:
            with open(".cache/lastest", "w") as f:
                f.write(c)
