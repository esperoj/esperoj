import subprocess
from functools import partial


def info(esperoj):
    """Execute commands "free -h, uname -a, lsb_release -a, lscpu, df -hT, curl https://ipwho.de" and show system info.

    Args:
        esperoj (object): An object passed from the parent function.

    Returns:
        None
    """

    free = subprocess.check_output("free -h", shell=True).decode("utf-8")
    uname = subprocess.check_output("uname -a", shell=True).decode("utf-8")
    lsb_release = subprocess.check_output("lsb_release -a", shell=True).decode("utf-8")
    lscpu = subprocess.check_output("lscpu", shell=True).decode("utf-8")
    df = subprocess.check_output("df -hT", shell=True).decode("utf-8")
    ipwho = subprocess.check_output("curl https://ipwho.de", shell=True).decode("utf-8")

    print("System Information:")
    print("-------------------")
    print("Free:")
    print(free)
    print("Uname:")
    print(uname)
    print("Lsb_release:")
    print(lsb_release)
    print("Lscpu:")
    print(lscpu)
    print("Df:")
    print(df)
    print("IPWho:")
    print(ipwho)


def get_esperoj_method(esperoj):
    """Create a partial function with esperoj object.

    Args:
        esperoj (object): An object to be passed as an argument to the partial function.

    Returns:
        functools.partial: A partial function with esperoj object bound to it.
    """
    return partial(info, esperoj)


def get_click_command():
    """Create a Click command for executing the info function.

    Returns:
        click.Command: A Click command object.
    """
    import click

    @click.command()
    @click.pass_obj
    def click_command(esperoj):
        """Execute the info function with the esperoj object.

        Args:
            esperoj (object): An object passed from the parent function.
        """
        info(esperoj)

    return click_command
