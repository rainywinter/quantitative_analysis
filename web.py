"""
使用flask框架，把基本面分析结果以web方式展示
"""

import base64
from io import BytesIO

from flask import Flask

# from matplotlib.figure import Figure

import fundamental
import load_data


app = Flask(__name__)


@app.route("/")
def root():
    return "Hi root"


@app.route("/<code>")
def hello(code):
    print("query code:", code)
    if code not in load_data.dt_a_share_codes:
        if name not in load_data.dt_a_share_names:
            return "股票代码错误"
        code = load_data.dt_a_share_names[code]
    fig = fundamental.core_indicator_plot(code, to_web=True)

    buf = BytesIO()
    # fig is instance of matplotlib.figure.Figure
    fig.savefig(buf, format="png")
    # Embed the result in the html output.
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return f"<img src='data:image/png;base64,{data}'/>"


if __name__ == "__main__":
    print("flask run begin")
    load_data.init_global()
    fundamental.init_global()

    print("flask run")

    app.run(debug=True)
