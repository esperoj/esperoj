"""Esperoj package."""


def nuitka() -> list:
    """Use to let nuitka knows what I need."""
    import esperoj.server

    return [esperoj.server]
