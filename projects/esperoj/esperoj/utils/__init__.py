"""Utils moduule."""

utils = {}


def get_util(name):
    if not (util := utils.get(name)):
        mod = __import__(f"esperoj.utils.{name}", None, None, [name])
        util = getattr(mod, name)
    return util


def nuitka():
    from esperoj.utils import calculate_hash, download, replicate, run_command, upload

    return [calculate_hash, download, replicate, run_command, upload]
