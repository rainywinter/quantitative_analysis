"""
akshare 同花顺API
"""

import akshare as ak


def all_industry():
    df = ak.stock_board_industry_name_ths()
    df.to_csv("data/ths_industry.csv", encoding="utf-8", index=False)


def industry_elements(symbol="中药"):
    df = ak.stock_board_industry_cons_ths(symbol=symbol)
    print(df)
    # df.to_csv(f"data/ths_industry_element/{symbol}.csv", encoding="utf-8", index=False)


def share_industry(code="000001"):
    ak.stock_zyjs_ths


if __name__ == "__main__":
    industry_elements()
