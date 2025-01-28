import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory

from esperoj.database.models import File
from esperoj.logging import get_logger
from esperoj.storage import get_mirror
from esperoj.utils import get_util

logger = get_logger(__name__)
calculate_hash = get_util("calculate_hash")
download = get_util("download")


def verify(files: list[File]) -> list[bool]:
    def verify_file(file) -> bool:
        result = True
        name = file.name
        start_time = time.time()
        logger.info("Started to verify file '%s'.", name)
        mirrors_items = list(file.mirrors.items())
        random.shuffle(mirrors_items)
        for mirror_name, mirror_info in mirrors_items:
            if not result:
                break
            for source in mirror_info["sources"]:
                src = source["src"]
                if source["sha256"] != calculate_hash(get_mirror(mirror_name).stream(src)):
                    result = False
                    logger.error("Verified failed for file '%s' of mirror '%s' with src '%s'.", name, mirror_name, src)
                    break
        with TemporaryDirectory() as tmpdirname:
            dest = Path(tmpdirname) / file.name
            download_info = {**dict(file), "dest": dest}
            error = download([download_info])[0][0]
            if error:
                result = False
                logger.error("Verified failed for file '%s'.", name)
        logger.info("Verified file '%s' in %d seconds.", name, time.time() - start_time)
        return result

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(verify_file, file): file for file in files}
        return [future.result() for future in as_completed(futures)]
