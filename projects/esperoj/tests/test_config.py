import json

from esperoj.config import get_config
from esperoj.log import get_logger

logger = get_logger(__name__)
logger.info(json.dumps(get_config()))
