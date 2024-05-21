"""
暂时无用
存储全局变量，方便引用，只初始化一次
"""

import load_data


def init():
    """全局变量"""
    # dataframe  A股股票代码、名称
    df_a_shares = load_a_shares()
    # dict 股票{code:name},{name:code}
    dt_a_share_codes, dt_a_share_names = a_shares_to_dict(df_a_shares)

    # 财报
    df_a_share_annual_finance_reports = load_a_annual_finance_reports()
