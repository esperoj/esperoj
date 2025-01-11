"""Utils moduule."""

utils = {}


def get_util(name):
    if not (util := utils.get(name)):
        mod = __import__(f"esperoj.utils.{name}", None, None, [name])
        util = mod.getattr(name)
    return util


def nuikta():
    from esperoj.utils import calculate_hash, run_command

    return [calculate_hash, run_command]
