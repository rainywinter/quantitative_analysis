参考github.com/wkingnet/stock-analysis解析通达信原始数据


## 旧版本通达信可免费下载专业财务数据包
https://data.tdx.com.cn/level2/history/new_tdx_history/new_tdx_7.603.exe

## pip国内镜像加速
```
pip install {packagename} -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com
```

安装步骤：
- 安装旧版通达信、下载财务数据包、下载日线数据
- git clone ...
- python3 -m venv .venv（创建venv）
- . .venv3/bin/activate (启动venv)
- 执行 pip install -r requirements.txt
- cp config.py.template config.py
- 配置config.py, a_share_path processed_data_root_path tdx_root_path
- 执行 python config.py
- 执行 python update.py
- 执行 python tdx_data_func.py