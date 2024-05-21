"""
项目运行所需配置文件
首先执行本文件以创建必须的目录
"""

import os

a_share_path = (
    "/Users/yc/Documents/code/stock/share_fundamental_analysis/data/a_share.csv"
)


# 通达信需要转换的指数文件。通达信按998查看重要指数
index_list = [
    "sh999999.day",  # 上证指数
    "sh000300.day",  # 沪深300
    "sz399001.day",  # 深成指
]


# 指定通达信数据目录
tdx_root_path = "/Users/yc/Documents/code/stock/share_fundamental_analysis/data"
# tdx_root_path = "/Users/yc/Documents/code/stock/tdx_data"


class TdxCfg:
    """
    通达信日线和财务数据下载之后保存在软件安装目录下vipdoc文件夹
    其中T0002文件夹保存了股本变更除权等数据

    其余目录为本软件保存分析后数据
    """

    # 数据根目录
    root_path = tdx_root_path
    # 通达信原始日线数据
    ori_lday_bj = tdx_root_path + os.sep + "vipdoc/bj/lday"
    ori_lday_sh = tdx_root_path + os.sep + "vipdoc/sh/lday"
    ori_lday_sz = tdx_root_path + os.sep + "vipdoc/sz/lday"
    # 通达信原始财务数据
    ori_cw = tdx_root_path + os.sep + "vipdoc/cw"
    # 前复权后csv格式的日线数据目录（单只股票单个文件保存，文件名为股票代码）
    lday_qfq = tdx_root_path + os.sep + "lday_qfq"
    # pickle格式日线数据保存目录
    pickle = tdx_root_path + os.sep + "pickle"
    # csv格式指数日线目录
    index = tdx_root_path + os.sep + "index"
    # 专业财务保存目录
    cw = tdx_root_path + os.sep + "cw"
    # 通达信原始股本变迁
    ori_gbbq = tdx_root_path + os.sep + "/T0002/hq_cache/gbbq"
    # 股本变迁保存目录
    gbbq = tdx_root_path + os.sep + "gbbq.csv"
    # 通达信正常交易状态的股票列表
    normal_shares = tdx_root_path + os.sep + "/T0002/hq_cache/infoharbor_ex.code"
    # 通达信服务器IP, 从通达信软件服务器列表中选一个最快的即可
    pytdx_ip = "124.70.133.119"
    # 通达信服务器端口
    pytdx_port = 7709
    # 通达信服务器文件校检信息txt
    gpcw_url = "http://down.tdx.com.cn:8001/tdxfin/gpcw.txt"
    # 通达信财务zip包url
    zip_url = "http://down.tdx.com.cn:8001/tdxfin/"


if __name__ == "__main__":
    # pre check and mkdir
    paths = [
        tdx_root_path,
        TdxCfg.cw,
        TdxCfg.lday_qfq,
        TdxCfg.index,
        TdxCfg.pickle,
    ]
    for p in paths:
        if not os.path.exists(p):
            os.mkdir(p)
            print(f"mkdir:{p}")
