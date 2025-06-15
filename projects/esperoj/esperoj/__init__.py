"""Esperoj package."""


def nuitka() -> list:
    """Use to let nuitka knows what I need."""
    import esperoj.database
    import esperoj.log
    import esperoj.server
    import esperoj.storage
    import esperoj.utils

    return [esperoj.server, esperoj.database, esperoj.utils, esperoj.storage, esperoj.log]
