import argparse
import sys
from typing import Type

from esperoj.cli.base import Command


def execute_command_from_cli_entrypoint(command_class: Type[Command]):
    """
    Executes a given Command class from a CLI entry point (like a script's __main__ block).

    This utility sets up an ArgumentParser, adds the command's specific arguments,
    parses `sys.argv` (excluding the script name), and then calls the command's
    `handle` method with the parsed options.

    Args:
        command_class: The Command subclass to be instantiated and executed.
    """
    if not (isinstance(command_class, type) and issubclass(command_class, Command) and command_class is not Command):
        raise TypeError(f"Expected a Command subclass, but got {command_class}")

    cmd_instance = command_class()
    parser = argparse.ArgumentParser(description=cmd_instance.help)
    cmd_instance.add_arguments(parser)

    # Parse arguments from sys.argv, excluding the script name itself.
    args = parser.parse_args(sys.argv[1:])

    options = {k: v for k, v in vars(args).items()}
    cmd_instance.handle(**options)
