import click
import subprocess


def info():
    """Execute commands "free -h, uname -a, lsb_release -a, lscpu, df -hT, curl https://ipwho.de" and show info"""

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


esperoj_method = info


@click.command()
def click_command():
    info()
