"""
akshare 东方财富API
"""

import akshare as ak
import pandas as pd
from tqdm import tqdm


def all_industry():
    """
    全部行业板块名称及板块代码
    保存csv
    """
    df = ak.stock_board_industry_name_em()
    df = df[["板块名称", "板块代码"]]
    df.sort_values(by="板块代码", inplace=True)
    df.to_csv("data/em_industry.csv", encoding="utf-8", index=False)


def industry_elements():
    """
    全部股票代码 名称 行业名称
    保存csv
    """
    df = pd.read_csv("data/em_industry.csv", encoding="utf-8")
    industry = df["板块名称"].to_list()

    df_elements = pd.DataFrame()
    for name in tqdm(industry, desc="行业板块"):
        df_tmp = ak.stock_board_industry_cons_em(symbol=name)
        df_tmp = df_tmp[["代码", "名称"]]
        df_tmp["所属板块"] = name
        df_elements = pd.concat([df_elements, df_tmp], axis=0)

    df_elements.sort_values(by="代码")
    df_elements.to_csv("data/em_industry_element.csv", encoding="utf-8", index=False)


def share_base_info(code="000001"):
    df = ak.stock_individual_info_em(symbol=code)
    print(df)


if __name__ == "__main__":
    # all_industry()
    industry_elements()
