import os
import time

import pandas as pd
import numpy as np
import backtrader as bt


if __name__ == "__main__":
    df1 = pd.DataFrame(np.arange(4).reshape(2, 2), columns=["a", "b"])
    df2 = pd.DataFrame(np.arange(4, 8).reshape(2, 2), columns=["a", "b"])
    print(df1)
    print(df2)

    print(pd.concat([df1, df2]))

    with open(".cache/lastest", "r") as f:
        line = f.read()
        print("lastest code", line)
        # index = codes.index(line)

    with open(".cache/lastest", "w") as f:
        f.write("000539")
