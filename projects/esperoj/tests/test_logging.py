from esperoj.log import getLogger

logger = getLogger(__name__)
logger.info("Hello World")
getLogger("test").critical("critical")
