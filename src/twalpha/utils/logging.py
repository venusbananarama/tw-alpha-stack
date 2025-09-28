from __future__ import annotations
from loguru import logger

def setup_logger():
    logger.remove()
    logger.add("pipeline.log", rotation="10 MB", retention="14 days", enqueue=True)
    logger.add(lambda msg: print(msg, end=""))
    return logger
