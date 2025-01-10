"""Esperoj package."""


def nuitka() -> list:
    """Use to let nuitka knows what I need."""
    import esperoj.server
    import esperoj.utils
    import esperoj.storage
    import esperoj.database
    return [esperoj.server, esperoj.database, esperoj.utils, esperoj.storage]
