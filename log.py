"""
日志模块
"""

from loguru import logger


# debug
def d(msg: str, *args, **kwargs):
    logger.debug(msg, args, kwargs)


# info
def i(msg: str, *args, **kwargs):
    logger.info(msg, args, kwargs)


# warning
def w(msg: str, *args, **kwargs):
    logger.warning(msg, args, kwargs)


# error
def e(msg: str, *args, **kwargs):
    logger.error(msg, args, kwargs)


# critical
def c(msg: str, *args, **kwargs):
    logger.critical(msg, args, kwargs)


if __name__ == "__main__":
    d("hello")
    i("hello")
    w("hello")
    e("hello")
    c("hello")
