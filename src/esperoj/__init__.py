"""Esperoj package.

This module provides the Esperoj package, which includes functionalities for creating an instance of the Esperoj class.

Classes:
    Esperoj: A class representing the Esperoj package.

Functions:
    create_esperoj: Create an instance of the Esperoj class.

"""

import os

from esperoj.esperoj import Esperoj
from esperoj.database.memory import MemoryDatabase


def create_esperoj(db=None) -> Esperoj:
    """Create an instance of the Esperoj class.

    Returns:
        Esperoj: An instance of the Esperoj class.

    Raises:
        None

    """
    config = {}
    if os.environ.get("ESPEROJ_DATABASE") == "Airtable":
        from esperoj.database.airtable import Airtable

        config["db"] = Airtable("Airtable")
    else:
        from esperoj.database.memory import MemoryDatabase

        if db is None:
            from esperoj.database.memory import MemoryDatabase
            config["db"] = MemoryDatabase("Memory Database")
        else:
            config["db"] = db
    from esperoj.storage.s3 import S3Storage

    config["storage"] = S3Storage(
        name="Storj",
        config={
            "client_config": {
                "aws_access_key_id": os.getenv("STORJ_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("STORJ_SECRET_ACCESS_KEY"),
                "endpoint_url": os.getenv("STORJ_ENDPOINT_URL"),
            },
            "bucket_name": "esperoj",
        },
    )
    return Esperoj(**config)


__all__ = ["Esperoj", "create_esperoj"]
