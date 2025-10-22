import argparse
import os
import sys
from importlib import import_module
from pathlib import Path


class Command:
    """Base class for CLI commands."""

    name: str
    help: str

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add arguments to the command's parser."""
        pass

    def handle(self, *args, **options):
        """Execute the command logic."""
        raise NotImplementedError("Subclasses must implement the handle method.")


def discover_commands(command_dir: Path) -> dict[str, Command]:
    """Discover commands in a given directory."""
    commands = {}
    # Ensure command_dir is treated as a package for importlib
    sys.path.insert(0, str(command_dir.parent))
    package_name = command_dir.name

    for f in command_dir.iterdir():
        if f.suffix == ".py" and f.name != "__init__.py":
            module_name = f.stem
            try:
                module = import_module(f"{package_name}.{module_name}")
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, Command) and attr is not Command:
                        cmd_instance = attr()
                        if cmd_instance.name:
                            commands[cmd_instance.name] = cmd_instance
                            print(f"Discovered command: {cmd_instance.name}")
                        break  # Assume one command class per file
            except Exception as e:
                print(f"Could not load command {module_name}: {e}", file=sys.stderr)
    sys.path.pop(0)
    return commands


def run_script_command(args):
    """Handle the 'run' subcommand to execute an arbitrary script."""
    script_path = Path(args.script)
    if not script_path.exists():
        print(f"Error: Script '{script_path}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Running script: {script_path} with Django setup.")

    # Add the script's directory to sys.path so it can be imported
    sys.path.insert(0, str(script_path.parent))
    try:
        # Execute the script in a way that respects Django setup
        # This is a bit tricky, but a common way is to run it as a module
        # or simply exec its contents. For simplicity, we'll exec.
        with open(script_path, "r") as f:
            script_code = f.read()
        exec(script_code, {"__name__": "__main__", "__file__": str(script_path)})
    except Exception as e:
        print(f"Error executing script '{script_path}': {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        sys.path.pop(0)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Esperoj CLI for various project tasks.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add the 'run' subcommand
    run_parser = subparsers.add_parser("run", help="Run an arbitrary Python script with Django environment.")
    run_parser.add_argument("script", type=str, help="Path to the Python script to run.")
    run_parser.set_defaults(func=run_script_command)

    # Discover and add internal commands
    cli_dir = Path(__file__).parent
    commands_dir = cli_dir / "commands"
    if not commands_dir.exists():
        os.makedirs(commands_dir)
        print(f"Created directory: {commands_dir}", file=sys.stderr)

    internal_commands = discover_commands(commands_dir)

    for name, cmd_instance in internal_commands.items():
        cmd_parser = subparsers.add_parser(name, help=cmd_instance.help)
        cmd_instance.add_arguments(cmd_parser)
        cmd_parser.set_defaults(func=cmd_instance.handle, cmd_instance=cmd_instance)

    args = parser.parse_args()

    if hasattr(args, "func"):
        if args.command == "run":
            args.func(args)
        else:
            # For internal commands, pass the parser arguments
            options = {k: v for k, v in vars(args).items() if k not in ["func", "command", "cmd_instance"]}
            args.cmd_instance.handle(**options)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
