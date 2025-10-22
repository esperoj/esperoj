import argparse
import os
import sys
from importlib import import_module, util
from pathlib import Path

from esperoj.cli.base import Command
from esperoj.cli.utils import execute_command_from_cli_entrypoint


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
    if not args.script_and_args:
        print("Error: No script path provided.", file=sys.stderr)
        sys.exit(1)

    script_path = Path(args.script_and_args[0])
    script_args = args.script_and_args[1:]

    if not script_path.exists():
        print(f"Error: Script '{script_path}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Running script: {script_path} with Django setup.")

    # Save original sys.argv and replace with script-specific arguments
    original_sys_argv = sys.argv
    sys.argv = [str(script_path)] + script_args

    # Add the script's directory to sys.path so it can be imported
    # (The parent directory is added so the module can find local imports if any)
    script_dir = str(script_path.parent)
    sys.path.insert(0, script_dir)

    module_name = script_path.stem
    try:
        # Create a module spec from the script file
        spec = util.spec_from_file_location(module_name, script_path)
        if spec is None:
            raise ImportError(f"Could not create module spec for {script_path}")

        # Ensure a loader is available for the spec
        if spec.loader is None:
            raise ImportError(f"No module loader found for script '{script_path}'")

        # Create a new module based on the spec
        module = util.module_from_spec(spec)

        # Set __file__ and __package__ for the module to mimic direct script execution
        # __name__ will be set by the loader to the module\'s actual name
        module.__file__ = str(script_path)
        module.__package__ = ""  # Scripts often run as top-level

        # Add the module to sys.modules
        sys.modules[module_name] = module

        # Execute the module\'s code
        spec.loader.exec_module(module)

        # Look for a specific MAIN_COMMAND attribute in the dynamically loaded module
        cmd_class = getattr(module, "MAIN_COMMAND", None)
        if cmd_class and isinstance(cmd_class, type) and issubclass(cmd_class, Command) and cmd_class is not Command:
            execute_command_from_cli_entrypoint(cmd_class)
        else:
            print(
                f"No valid MAIN_COMMAND (subclass of Command) found in module {module_name}. Script not executed as a command.",
                file=sys.stderr,
            )

    except Exception as e:
        print(f"Error executing script '{script_path}': {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        sys.path.pop(0)
        sys.argv = original_sys_argv  # Restore original sys.argv
        # Clean up sys.modules if the module was added, to avoid side effects
        if module_name in sys.modules:
            del sys.modules[module_name]


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Esperoj CLI for various project tasks.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add the 'run' subcommand
    run_parser = subparsers.add_parser("run", help="Run an arbitrary Python script with Django environment.")
    run_parser.add_argument(
        "script_and_args", nargs=argparse.REMAINDER, help="Path to the Python script to run, followed by its arguments."
    )
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
