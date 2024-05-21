"""
通达信数据函数
"""

import os
import time
import struct
from struct import unpack
from decimal import Decimal
from datetime import datetime
import requests

from tqdm import tqdm
import pandas as pd
import numpy as np
import pytdx.reader.gbbq_reader
from pytdx.reader import HistoryFinancialReader
from retry import retry

import config as cfg
import log
import load_data


@retry(tries=3, delay=1)
def download(url):
    """
    http.get，可重试下载
    :return (response.content, response.text)
    """
    res = requests.get(url)
    res.raise_for_status()
    return res.content, res.text


def lday_to_csv(src="", dst="", file_name=""):
    """
    将通达信vipdoc目录下，bj sh sz（北交所、上交所、深交所）日线数据原始二进制格式，转换为DataFrame格式存储到csv文件中。
    通达信数据文件32字节为一组
    支持增量更新
    :param src: str 二进制文件路径
    :param dst: str csv文件保存路径
    :param file_name: str 二进制文件名。example: sh000001.day
    :return none
    """
    code = file_name[2:-4]
    src_path = src + os.sep + file_name
    dst_path = dst + os.sep + code + ".csv"
    with open(src_path, "rb") as f:
        buf = f.read()

    chunk_size = 32
    chunk_num = int(len(buf) / chunk_size)
    buf_index = 0

    dst_exist = os.path.isfile(dst_path)
    with open(dst_path, "a+", encoding="utf-8") as dst_file:
        # 写表头
        header_f = lambda: dst_file.write("date,code,open,high,low,close,vol,amount")
        if not dst_exist:
            header_f()
            total_row = 0
        else:
            dst_file.seek(0, 0)
            total_row = len(dst_file.readlines())
            # 创建了文件，写表头失败，重新写入
            if total_row == 0:
                header_f()
                total_row = 1
                return
            else:
                dst_file.seek(0, 2)

            # 除去第一行表头行
            total_row = total_row - 1
            buf_index = total_row * chunk_size

        for _ in range(total_row, chunk_num):
            # 将字节流转换成Python数据格式
            # I: unsigned int
            # f: float
            # [5]浮点类型的成交金额，使用decimal类四舍五入为整数
            info = unpack("IIIIIfII", buf[buf_index : buf_index + chunk_size])
            date = datetime.strptime(str(info[0]), "%Y%m%d").strftime("%Y-%m-%d")
            content = ["\n" + date, code]
            for i in range(1, 5):
                content.append(str(info[i] / 100.0))
            content.append(str(info[6]))
            amount = Decimal(info[5]).quantize(Decimal("1."), rounding="ROUND_HALF_UP")
            content.append(str(amount))

            dst_file.write(",".join(content))
            buf_index += chunk_size


def backward_adjust(lday_path="", df_gbbq=pd.DataFrame):
    """
    前复权 涨跌幅复权法
    在每次除权发生后， 根据除权价和前一收盘价计算一个比率，称为除权因子；把截止到计算日历次的除权因子连乘，即为截止日的累积除权因子。计算前复权价，则以价格乘上累积除权因子；向后复权，则价格除以累积除权因子

    对导出的日线数据结合股本变迁数据进行前复权处理
    :param lday_path: str 日线文件路径
    :param df_gbbq: str 股本变迁dataframe

    复权处理后，将新增adj列，值为True/False，表示是否复权

    根据除权除息/股本变化等计算复权因子（根据该因子可直接计算出股价）
    已经计算过的因子不需重复计算
    """
    log.i("开始前复权")

    files = os.listdir(lday_path)
    files.sort()
    for filename in tqdm(files, desc="前复权:"):
        # example: 000001.csv
        code = filename[:-4]
        df_src = pd.read_csv(
            lday_path + os.sep + filename, encoding="utf-8", dtype={"code": str}
        )
        df_src["date"] = pd.to_datetime(df_src["date"], format="%Y-%m-%d")
        df_src.set_index("date", drop=True, inplace=True)
        # 非交易时间（停牌期间等）可除权除息/变更股本，添加trade列方便处理非交易日期的除权除息
        df_src["trade"] = True

        if "adj" in df_src.columns.to_list():
            if True in df_src["adj"].isna().to_list():
                begin_row = np.where(df_src["adj"].isna())[0][0]
            else:
                # 已复权，跳过
                continue
        else:
            begin_row = 0
            new_share = True

        # 从头计算复权因子 还是增量计算后来未复权的数据
        # 未复权的数据中 不包含除权除息数据才能增量计算
        append = False

        df_share = df_gbbq[df_gbbq["code"] == code]
        # 新增date，方便后续设置索引合并日线dataframe , load_date 中已经设置过date
        # df_share.loc[:, ["date"]] = pd.to_datetime(df_share["权息日"], format="%Y%m%d")

        # 除权除息 exclude right/dividend
        df_xrxd = df_share[df_share["类别"] == "除权除息"]
        # 股本变化
        df_capital = df_share[
            (df_share["类别"] == "股本变化")
            | (df_share["类别"] == "送配股上市")
            | (df_share["类别"] == "转配股上市")
        ]

        # 送配股上市 送配股上市 转配股上市可能在同一天发生，需要保留[送转股-后流通盘]值最大的行，其余的同天数据需要丢弃。
        # 同天使用最新值即可，无需重复计算，且下文使用日期做索引时会冲突，综上2点，需要过滤数据
        # 做法：先升序排序，再保留重复日期的最后一条
        df_capital = df_capital.sort_values(by="送转股-后流通盘")
        df_capital = df_capital.drop_duplicates(subset=["权息日"], keep="last")

        # 前已新增date列，设置date为索引以合并日线dataframe
        df_xrxd.set_index("date", drop=True, inplace=True)
        df_capital.set_index("date", drop=True, inplace=True)
        df_capital = df_capital.rename(columns={"送转股-后流通盘": "流通股"})

        # 最新的除权除息日
        if df_xrxd.empty:
            xrxd_lastest = pd.Timestamp.today().normalize()
        else:
            xrxd_lastest = df_xrxd.index[-1]
        df_proceed = pd.DataFrame()
        if begin_row > 0 and xrxd_lastest < df_src.index[begin_row]:
            append = true
            df_proceed = df_src
            df_src = df_src.iloc[begin_row:]
            df_proceed.dropna(how="any", inplace=True)

        """
        df_dst为最终要写入文件的dataframe
        因为日线数据只记录交易日的数据
        category列为了在非交易日除权除息而引入的辅助列。名字和值无特别意义（不为na即可）
        含非交易日的df_xrxd数据合并到日线df_dst时，df_dst会新增行，产生的na数据需要向下填充（即需要复制前一个交易日的open close、high等数据）。
        之后再并入df_xrxd分红、配股等列时，df_dst不会再产生新行。对并入产生的na值可以填充为0，才能计算复权因子。最后再剔除category列和非交易日的行（trade为false）
        """
        df_xrxd.loc[:, ["category"]] = 1.0
        df_dst = pd.concat([df_src, df_xrxd[["category"]][df_src.index[0] :]], axis=1)
        # avoid warnning
        with pd.option_context("future.no_silent_downcasting", True):
            # 非交易日的除权除息填充 trade=False
            df_dst.fillna({"trade": False}, inplace=True)
            # 非交易日的open/close/high 等数据填充为上一个交易日的数据
            df_dst.ffill(inplace=True)

        # 合并除权除息列
        df_dst = pd.concat(
            [
                df_dst,
                df_xrxd[
                    [
                        "分红-前流通盘",
                        "配股-后总股本",
                        "配股价-前总股本",
                        "送转股-后流通盘",
                    ]
                ][df_src.index[0] :],
            ],
            axis=1,
        )
        # 交易日的 分红、配股、配股价、送转股填充为0
        df_dst.fillna(value=0, inplace=True)

        # 除权收盘价 = 除权除息价 = (股权登记日的收盘价-每股所分红利现金额+配股价×每股配股数)÷(1+每股送红股数+每股配股数+每股转增股数)
        # 除权因子 = 除权收盘价 / 除权登记日收盘价
        # 财报中皆以10为单位分红、配转股
        df_dst["xrxd_close"] = (
            df_dst["close"].shift(1) * 10
            - df_dst["分红-前流通盘"]
            + df_dst["配股-后总股本"] * df_dst["配股价-前总股本"]
        ) / (10 + df_dst["配股-后总股本"] + df_dst["送转股-后流通盘"])
        # 复权因子
        df_dst["adj"] = (
            (df_dst["xrxd_close"].shift(-1) / df_dst["close"])
            .fillna(value=1)[::-1]
            .cumprod()
        )
        df_dst["open"] = df_dst["open"] * df_dst["adj"]
        df_dst["high"] = df_dst["high"] * df_dst["adj"]
        df_dst["low"] = df_dst["low"] * df_dst["adj"]
        df_dst["close"] = df_dst["close"] * df_dst["adj"]

        # 去除trade 为false的行，只保留交易日的数据
        df_dst = df_dst[df_dst["trade"]]

        # 去除引入的计算复权因子的中间列
        df_dst = df_dst.drop(
            [
                "分红-前流通盘",
                "配股-后总股本",
                "配股价-前总股本",
                "送转股-后流通盘",
                "trade",
                "category",
                "xrxd_close",
            ],
            axis=1,
        )[df_dst["open"] != 0]

        # 价格 round
        df_dst = df_dst.round(
            {
                "open": 2,
                "high": 2,
                "low": 2,
                "close": 2,
            }
        )

        # TODO 流通股、流通市值、换手率

        if not df_proceed.empty:
            df_dst = df_proceed.append(df_dst)
        df_dst.reset_index(drop=False, inplace=True)

        df_dst.to_csv(cfg.TdxCfg.lday_qfq + os.sep + filename, encoding="utf-8")
        df_dst.to_pickle(cfg.TdxCfg.pickle + os.sep + filename[:-4] + ".pkl")
        # return


def gbbq_to_csv(src_path="", dst_path=""):
    """
    从通达信中解析股本变迁
    :param src_path: str 通达信股本变迁原文件
    :param dst_path: str 解析后的股本变迁文件路径
    """
    log.i("开始处理股本变迁")
    start = time.time()

    category = {
        "1": "除权除息",
        "2": "送配股上市",
        "3": "非流通股上市",
        "4": "未知股本变动",
        "5": "股本变化",
        "6": "增发新股",
        "7": "股份回购",
        "8": "增发新股上市",
        "9": "转配股上市",
        "10": "可转债上市",
        "11": "扩缩股",
        "12": "非流通股缩股",
        "13": "送认购权证",
        "14": "送认沽权证",
    }

    df_gbbq = pytdx.reader.gbbq_reader.GbbqReader().get_df(src_path)
    df_gbbq.drop(columns=["market"], inplace=True)
    df_gbbq.columns = [
        "code",
        "权息日",
        "类别",
        "分红-前流通盘",
        "配股价-前总股本",
        "送转股-后流通盘",
        "配股-后总股本",
    ]
    df_gbbq["类别"] = df_gbbq["类别"].astype("object")
    df_gbbq["code"] = df_gbbq["code"].astype("object")
    for i in range(df_gbbq.shape[0]):
        df_gbbq.iat[i, df_gbbq.columns.get_loc("类别")] = category[
            str(df_gbbq.iat[i, df_gbbq.columns.get_loc("类别")])
        ]
    df_gbbq.to_csv(dst_path, encoding="utf-8", index=False)

    log.i(f"前复权:end,用时{(time.time() - start):.2f}秒")


def load_cw_dat(path):
    """
    :param path :str: dat file path

    :return: DataFrame

    已参考了别人的读取写了读取的代码，这里又直接使用了pytdx库直接读，稍微处理下列做兼容
    """

    """
    方案一：
    使用pytdx.reader 直接读取
    df = HistoryFinancialReader().get_df(path)
    # 将日期移动到最后
    date = df["report_date"].to_list()
    df.drop("report_date", axis=1, inplace=True)

    # 索引code自动变为第一列
    df = df.reset_index(drop=False)
    df.columns = [i for i in range(df.shape[1])]
    df["report_date"] = date
    return df
    """

    # 方案二
    # 参考c语言数据结构finance_define.c，手动读取
    header_format = "<hIH3L"
    header_size = struct.calcsize(header_format)
    share_header_chunk_format = "<6scL"
    share_header_chunk_size = struct.calcsize(share_header_chunk_format)
    with open(path, "rb") as file:
        # shares_header example: (1, 20220331, 4831, 720896, 2324, 0)
        shares_header = unpack(header_format, file.read(header_size))
        date = shares_header[1]
        total = shares_header[2]
        cw_chunk_size = shares_header[4]
        cw_chunk_format = "<{}f".format(int(cw_chunk_size / 4))

        datas = []
        """
        参考pytdx reader库的实现
        此方案缺点：较多的前后seek，可能有性能问题
        for index in range(total):
            file.seek(header_size + index * share_header_chunk_size)
            item = unpack(share_header_chunk_format, file.read(share_header_chunk_size))
            code = item[0].decode("utf-8")
            length = item[2]

            file.seek(length)
            cw = list(unpack(cw_chunk_format, file.read(cw_chunk_size)))
            cw.insert(0, code)

            datas.append(cw)
        """
        # [(code,offset)] (股票代码,文件偏移量)
        code_offsets = []
        for index in range(total):
            share_header = unpack(
                share_header_chunk_format, file.read(share_header_chunk_size)
            )
            code, offset = share_header[0].decode("utf-8"), share_header[2]
            code_offsets.append((code, offset))

        for code_of in code_offsets:
            code, offset = code_of[0], code_of[1]
            if file.tell() != offset:
                print(
                    f"{code}有填充，offset不一致,recode={offset},current_offset={file.tell()}"
                )
                file.seek(offset)
            cw = list(unpack(cw_chunk_format, file.read(cw_chunk_size)))
            cw.insert(0, code)

            datas.append(cw)

        df = pd.DataFrame(datas)
        return df


if __name__ == "__main__":
    # gbbq_to_csv(cfg.TdxCfg.ori_gbbq, "gbbq.csv")

    # content = str(download(cfg.TdxCfg.gpcw_url)).strip()
    # print(content)
    # with open("gpcw.txt", "w") as f:
    #     f.write(content)

    # with open(
    #     "/Users/yc/Documents/code/stock/share_fundamental_analysis/gpcw.txt", "r"
    # ) as f:
    #     content = str(f.read())

    # _, content = download(cfg.TdxCfg.gpcw_url)
    # lines = content.split("\r\n")
    # data = [l.strip().split(",") for l in lines]
    # df = pd.DataFrame(data, columns=["filename", "md5", "filesize"])
    # print(df)

    # path = cfg.TdxCfg.ori_cw + "/gpcw20220331.dat"
    # df = load_cw_dat(path)

    # print(df)
    df = load_data.load_gbbq()
    backward_adjust(cfg.TdxCfg.lday_qfq, df)
