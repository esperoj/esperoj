import logging

loggers = {}
defaultFormatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
defaultHandler = logging.StreamHandler()
defaultHandler.setFormatter(defaultFormatter)


def getLogger(name):
    if not (logger := loggers.get(name)):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(defaultHandler)
        loggers[name] = logger
    return logger
