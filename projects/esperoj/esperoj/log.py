import logging

loggers = {}
default_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
default_handler = logging.StreamHandler()
default_handler.setFormatter(default_formatter)


def get_logger(name):
    if not (logger := loggers.get(name)):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(default_handler)
        loggers[name] = logger
    return logger
