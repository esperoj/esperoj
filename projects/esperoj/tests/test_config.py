import json

from esperoj.config import get_config
from esperoj.logging import get_logger

logger = get_logger(__name__)
logger.info(json.dumps(get_config()))
