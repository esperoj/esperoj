import argparse

import sys
from pathlib import Path

import pytest
from esperoj.cli.base import Command
from esperoj.cli.main import discover_commands, main
from esperoj.cli.utils import execute_command_from_cli_entrypoint


# --- Dummy Commands and Scripts for Testing ---


class DummyCommand(Command):
    name = "dummy"
    help = "A dummy command for testing."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("--message", type=str, default="Default", help="A test message.")
        parser.add_argument("--value", type=int, default=0, help="A test value.")

    def handle(self, **options):
        print(f"DummyCommand handled: message={options['message']}, value={options['value']}")
        return options  # Return options for inspection in tests


class ErrorCommand(Command):
    name = "error"
    help = "A command that raises an error."

    def handle(self, **options):
        raise ValueError("This is an intentional error from ErrorCommand.")


# A dummy script that defines a MAIN_COMMAND
DUMMY_SCRIPT_CONTENT = """
import argparse
import sys
from esperoj.cli.base import Command
from esperoj.cli.utils import execute_command_from_cli_entrypoint

class MyScriptCommand(Command):
    name = "myscript"
    help = "A script-defined command."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument("--script-arg", type=str, default="script_default", help="An argument for the script.")
        parser.add_argument("--script-val", type=int, default=10, help="Another argument for the script.")

    def handle(self, **options):
        print(f"MyScriptCommand handled: script_arg={options['script_arg']}, script_val={options['script_val']}")
        return options

MAIN_COMMAND = MyScriptCommand

if __name__ == "__main__":
    execute_command_from_cli_entrypoint(MAIN_COMMAND)
"""

# --- Fixtures ---


@pytest.fixture
def cli_commands_dir(tmp_path: Path):
    """Fixture to create a temporary 'commands' directory for CLI discovery."""
    commands_dir = tmp_path / "commands"
    commands_dir.mkdir()
    return commands_dir


@pytest.fixture
def create_dummy_command_file(cli_commands_dir: Path):
    """Factory fixture to create a dummy command file for discovery."""

    def _create_file(name: str, content: str):
        file_path = cli_commands_dir / f"{name}.py"
        file_path.write_text(content)
        return file_path

    return _create_file


@pytest.fixture
def mock_sys_argv():
    """Mocks sys.argv for testing CLI commands."""
    original_argv = sys.argv
    try:
        sys.argv = ["esperoj"]  # Default to just the program name
        yield sys.argv
    finally:
        sys.argv = original_argv


@pytest.fixture
def capsys_output(capsys):
    """Fixture to capture stdout/stderr output."""
    yield capsys
    # Ensure capture is reset for subsequent tests if not explicitly done


# --- Tests for esperoj.cli.base ---
# (Command class is mostly an interface, tested by its usage in subclasses)


def test_command_interface_handle_not_implemented():
    class IncompleteCommand(Command):
        name = "incomplete"
        help = "An incomplete command."
        # Missing handle method

    cmd = IncompleteCommand()
    with pytest.raises(NotImplementedError, match="Subclasses must implement the handle method."):
        cmd.handle()


# --- Tests for esperoj.cli.main ---


def test_discover_commands_basic(cli_commands_dir: Path, create_dummy_command_file):
    """Test discovery of a single Command subclass."""
    content = """
from esperoj.cli.base import Command

class TestCmd(Command):
    name = "testcmd"
    help = "A test command."
    def add_arguments(self, parser): pass
    def handle(self, **options): pass
"""
    create_dummy_command_file("testcmd", content)

    commands = discover_commands(cli_commands_dir)
    assert "testcmd" in commands
    assert isinstance(commands["testcmd"], Command)
    assert commands["testcmd"].name == "testcmd"


def test_discover_commands_no_command_class(cli_commands_dir: Path, create_dummy_command_file):
    """Test discovery when a file contains no Command subclass."""
    content = """
def some_function():
    pass
"""
    create_dummy_command_file("nocmd", content)

    commands = discover_commands(cli_commands_dir)
    assert "nocmd" not in commands


def test_discover_commands_multiple_command_classes_in_file(
    cli_commands_dir: Path, create_dummy_command_file, capsys_output
):
    """Test discovery with multiple command classes in one file (first one should be picked)."""
    content = """
from esperoj.cli.base import Command

class FirstCmd(Command):
    name = "first"
    help = "First command."
    def add_arguments(self, parser): pass
    def handle(self, **options): pass

class SecondCmd(Command):
    name = "second"
    help = "Second command."
    def add_arguments(self, parser): pass
    def handle(self, **options): pass
"""
    create_dummy_command_file("multi_cmd", content)

    commands = discover_commands(cli_commands_dir)
    assert "first" in commands
    assert "second" not in commands  # Only the first one should be picked due to 'break'
    assert commands["first"].name == "first"
    # Ensure no error messages about loading multiple commands
    captured = capsys_output.readouterr()
    assert "Could not load command" not in captured.err


def test_main_runs_discovered_command(
    cli_commands_dir: Path, create_dummy_command_file, mock_sys_argv, capsys_output, mocker
):
    """Test that the main CLI entry point can run a discovered command."""
    # Ensure DummyCommand is available for discovery
    dummy_cmd_file = cli_commands_dir / "dummy_command.py"
    dummy_cmd_file.write_text(
        """
from esperoj.cli.base import Command

class DummyCommand(Command):
    name = "test-dummy"
    help = "A temporary dummy command."
    def add_arguments(self, parser):
        parser.add_argument("--name", type=str, default="World")
    def handle(self, **options):
        print(f"Hello, {options['name']} from test-dummy!")
"""
    )

    mock_sys_argv.extend(["test-dummy", "--name", "Tester"])

    # Patch discover_commands to use our fixture directly, bypassing Path mocking in main
    mocker.patch(
        "esperoj.cli.main.discover_commands",
        return_value=discover_commands(cli_commands_dir),  # Call original discover_commands with our fixture
    )
    # Patch os.makedirs to prevent creating real directories
    mocker.patch("os.makedirs")

    main()

    captured = capsys_output.readouterr()
    assert "Discovered command: test-dummy" in captured.out
    assert "Hello, Tester from test-dummy!" in captured.out


def test_main_run_subcommand_script_not_found(cli_commands_dir: Path, mock_sys_argv, capsys_output):
    """Test 'run' subcommand behavior when script path is invalid."""
    mock_sys_argv.extend(["run", "non_existent_script.py"])

    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1

    captured = capsys_output.readouterr()
    assert "Error: Script 'non_existent_script.py' not found." in captured.err


def test_main_run_subcommand_executes_script(
    cli_commands_dir: Path, create_dummy_command_file, mock_sys_argv, capsys_output, mocker
):
    """Test 'run' subcommand successfully executes a script with MAIN_COMMAND."""
    script_file_path = create_dummy_command_file("my_test_script", DUMMY_SCRIPT_CONTENT)

    mock_sys_argv.extend(["run", str(script_file_path), "--script-arg", "custom", "--script-val", "99"])

    # Patch discover_commands for main() to avoid issues with commands_dir creation
    mocker.patch(
        "esperoj.cli.main.discover_commands",
        return_value={},  # No internal commands needed for this specific test
    )
    mocker.patch("os.makedirs")  # Prevent creating real directories

    main()

    captured = capsys_output.readouterr()
    assert f"Running script: {script_file_path} with Django setup." in captured.out
    assert "MyScriptCommand handled: script_arg=custom, script_val=99" in captured.out
    assert "No valid MAIN_COMMAND" not in captured.err  # Ensure it found the command


def test_main_run_subcommand_script_without_main_command(
    cli_commands_dir: Path, create_dummy_command_file, mock_sys_argv, capsys_output, mocker
):
    """Test 'run' subcommand for a script that doesn't define MAIN_COMMAND."""
    script_content = "print('Hello from script without MAIN_COMMAND')"
    script_file_path = create_dummy_command_file("plain_script", script_content)

    mock_sys_argv.extend(["run", str(script_file_path)])

    # Patch discover_commands for main() to avoid issues with commands_dir creation
    mocker.patch(
        "esperoj.cli.main.discover_commands",
        return_value={},  # No internal commands needed for this specific test
    )
    mocker.patch("os.makedirs")  # Prevent creating real directories

    main()

    captured = capsys_output.readouterr()
    assert "Hello from script without MAIN_COMMAND" in captured.out
    assert (
        "No valid MAIN_COMMAND (subclass of Command) found in module plain_script. Script not executed as a command."
        in captured.err
    )


def test_main_no_command_provided(mock_sys_argv, capsys_output, mocker):
    """Test main() with no subcommand, expecting help message."""
    # Patch discover_commands to ensure no commands are found, so default help is shown
    mocker.patch("esperoj.cli.main.discover_commands", return_value={})
    mocker.patch("os.makedirs")

    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    captured = capsys_output.readouterr()
    assert "usage: esperoj [-h] {run} ..." in captured.out  # Should list only 'run' now
    assert "Available commands" in captured.out


# --- Tests for esperoj.cli.utils ---


def test_execute_command_from_cli_entrypoint_success(capsys_output, mock_sys_argv):
    """Test execute_command_from_cli_entrypoint with a valid command."""
    mock_sys_argv[:] = ["test_script.py", "--message", "hello", "--value", "123"]

    execute_command_from_cli_entrypoint(DummyCommand)

    captured = capsys_output.readouterr()
    assert "DummyCommand handled: message=hello, value=123" in captured.out


def test_execute_command_from_cli_entrypoint_invalid_class():
    """Test execute_command_from_cli_entrypoint with an invalid class."""

    class NotACommand:
        pass

    with pytest.raises(TypeError, match="Expected a Command subclass"):
        execute_command_from_cli_entrypoint(NotACommand)  # type: ignore


def test_execute_command_from_cli_entrypoint_command_raises_error(capsys_output, mock_sys_argv):
    """Test execute_command_from_cli_entrypoint when the command's handle method raises an error."""
    mock_sys_argv[:] = ["test_script.py"]  # No specific args for error command needed

    with pytest.raises(ValueError, match="This is an intentional error from ErrorCommand."):
        execute_command_from_cli_entrypoint(ErrorCommand)

    # Check if any part of the error message was printed to stderr before the exception
    captured = capsys_output.readouterr()
    assert "ErrorCommand" not in captured.err  # Exception is re-raised, not printed by utility
