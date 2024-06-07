"""
更新日线、财务数据
python update.py
"""

import io
import os
import sys
import time
import hashlib
import zipfile

import akshare as ak
import pandas as pd
from tqdm import tqdm

import config as cfg
import log
import tdx_data_func
import load_data


def update_a_shares():
    """
    更新A股全部股票代码-股票名称
    网络原因，接口可能不稳定，报错时重试几次
    """
    start = time.time()
    log.i("更新A股全部股票代码-股票名称:start")

    df = ak.stock_info_a_code_name()
    df.set_index("code", drop=True, inplace=True)
    # 加上上市日期
    lt = []
    for filename in tqdm(
        os.listdir(cfg.ProcessedDataPath.tdx_lday_qfq), desc="query ipo date"
    ):
        code = filename[:-4]
        df_lday = load_data.load_line_day(code)
        if df_lday.empty:
            continue
        lt.append({"code": code, "listing_date": df_lday.loc[0, "date"]})

    df_listing = pd.DataFrame.from_dict(lt)
    df_listing.sort_values("code", inplace=True)
    df_listing.set_index("code", drop=True, inplace=True)

    df = pd.concat([df, df_listing], axis=1)

    df.reset_index(drop=False, inplace=True)
    df.to_csv(cfg.a_share_path, index=False, encoding="utf-8")

    log.i(
        f"更新A股全部股票代码-股票名称:end,共{df.shape[0]}份 用时{(time.time() - start):.2f}秒"
    )


def update_tdx_cw():
    """
    更新通达信财务数据
    """
    start = time.time()
    log.i("更新通达信财务数据:start")
    _, text = tdx_data_func.download(cfg.TdxCfg.gpcw_url)
    data = [l.strip().split(",") for l in text.strip().split("\r\n")]
    df_check_sum = pd.DataFrame(data, columns=["filename", "md5", "filesize"])
    list_md5 = df_check_sum["md5"].to_list()

    ori_files = [name for name in os.listdir(cfg.TdxCfg.ori_cw) if name[-3:] == "zip"]
    list_not_exist = []
    list_not_equal = []
    download_task = []

    # 计算需要下载的zip文件
    for index, filename in enumerate(df_check_sum["filename"].to_list()):
        path = cfg.TdxCfg.ori_cw + os.sep + filename
        if filename in ori_files:
            md5 = hashlib.md5(open(path, "rb").read()).hexdigest()
            if md5 != list_md5[index]:
                download_task.append(filename)
                list_not_equal.append(filename)
        else:
            download_task.append(filename)
            list_not_exist.append(filename)

    # 1 downaload zip 2 extract zip
    for filename in tqdm(download_task, desc="download task:"):
        content, _ = tdx_data_func.download(cfg.TdxCfg.zip_url + filename)
        # extract
        with zipfile.ZipFile(io.BytesIO(content)) as f:
            f.extractall(cfg.TdxCfg.ori_cw)

        with open(cfg.TdxCfg.ori_cw + os.sep + filename, "wb") as f:
            f.write(content)

    # make sure all zip had been extracted
    zip_files = [
        name[:-4] for name in os.listdir(cfg.TdxCfg.ori_cw) if name[-3:] == "zip"
    ]
    dat_files = [
        name[:-4] for name in os.listdir(cfg.TdxCfg.ori_cw) if name[-3:] == "dat"
    ]
    for filename in tqdm(set(zip_files).difference(set(dat_files)), desc="extract:"):
        with zipfile.ZipFile(
            cfg.TdxCfg.ori_cw + os.sep + filename + ".zip", "r"
        ) as file:
            file.extractall(cfg.TdxCfg.ori_cw)
    # convert  all to pkl
    pkl_files = [name[:-4] for name in os.listdir(cfg.ProcessedDataPath.tdx_cw)]
    for filename in tqdm(
        set(zip_files).difference(set(pkl_files)), desc="convert to pkl:"
    ):
        ori_dat_path = cfg.TdxCfg.ori_cw + os.sep + filename + ".dat"
        dst_pkl_path = cfg.ProcessedDataPath.tdx_cw + os.sep + filename + ".pkl"
        df = tdx_data_func.load_cw_dat(ori_dat_path)
        df.to_pickle(dst_pkl_path, compression=None)

    log.i(
        f"更新通达信财务数据:start,共{len(list_not_equal)+len(list_not_exist)}份，其中not exist:{list_not_exist}, not equal:{list_not_equal} 用时{(time.time() - start):.2f}秒"
    )


def update_tdx_lday():
    """
    在通达信软件下载日线数据后，转换为csv格式
    并根据股本变迁数据进行前复权处理
    """
    start = time.time()
    log.i("更新通达信日线数据:start")

    # 1、沪市A股票买卖的代码是以600、601或603开头，新股申购代码以730、780或732开头；
    # 2、沪市科创板股票买卖的代码是以688开头，新股申购代码以787开头；
    # 3、深市A股票买卖的代码是以000-004开头，创业板股票代码以30开头；
    # 4、沪市B股代码是以90开头，深市B股代码是以20开头；
    # 5、新三板（基础层、创新层）与北交所股票代码一般为43、83、87开头。
    """
    df_shares = pd.read_csv(
        cfg.TdxCfg.normal_shares,
        sep="|",
        header=None,
        index_col=None,
        encoding="gbk",
        dtype={0: str},
    )
    shares_sh = df_shares[df_shares[0].str.startswith("6")][0]
    shares_sz = df_shares[0][
        df_shares[0].apply(lambda x: x[0:1] == "0" or x[0:1] == "3")
    ]
    shares_bj = df_shares[0][
        df_shares[0].str.startswith("8") | df_shares[0].str.startswith("43")
    ]
    """
    dst = cfg.ProcessedDataPath.tdx_lday_qfq

    log.i("导出深市日线")
    names = [
        name
        for name in os.listdir(cfg.TdxCfg.ori_lday_sz)
        if name[2:4] == "00" or name[2:4] == "30"
    ]
    names.sort()
    for name in tqdm(names, desc="sz lday:"):
        tdx_data_func.lday_to_csv(src=cfg.TdxCfg.ori_lday_sz, dst=dst, file_name=name)

    log.i("导出沪市日线")
    names = [
        name
        for name in os.listdir(cfg.TdxCfg.ori_lday_sh)
        if name[2:4] == "60" or name[2:4] == "68"
    ]
    names.sort()
    for name in tqdm(names, desc="sh lday"):
        tdx_data_func.lday_to_csv(src=cfg.TdxCfg.ori_lday_sh, dst=dst, file_name=name)

    log.i("导出指数")
    for name in tqdm(cfg.index_list, desc="index lday"):
        if name.startswith("sh"):
            tdx_data_func.lday_to_csv(
                src=cfg.TdxCfg.ori_lday_sh,
                dst=cfg.ProcessedDataPath.tdx_index,
                file_name=name,
            )
        elif name.startswith("sz"):
            tdx_data_func.lday_to_csv(
                src=cfg.TdxCfg.ori_lday_sz,
                dst=cfg.ProcessedDataPath.tdx_index,
                file_name=name,
            )

    # 暂时忽略北交所
    log.i(f"更新通达信日线数据:end,用时{(time.time() - start):.2f}秒")

    # 更新日线数据之后进行前复权
    start = time.time()
    log.i("前复权:start")
    df_gbbq = load_data.load_gbbq()
    tdx_data_func.backward_adjust(cfg.ProcessedDataPath.tdx_lday_qfq, df_gbbq=df_gbbq)
    log.i(f"前复权:end,用时{(time.time() - start):.2f}秒")


if __name__ == "__main__":
    print("argv: ", sys.argv)
    func_list = []
    if len(sys.argv) > 1:
        if sys.argv[1] == "all":
            func_list = [update_a_shares, update_tdx_cw, update_tdx_lday]
        elif sys.argv[1] == "shares":
            func_list = [update_a_shares]
        elif sys.argv[1] == "cw":
            func_list = [update_tdx_cw]
        elif sys.argv[1] == "lday":
            func_list = [update_tdx_lday]
    if len(func_list) == 0:
        print("参数错误")
    else:
        for func in func_list:
            func()
    print("exit update.py")
