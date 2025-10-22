from esperoj.cli.base import Command
from esperoj.cli.utils import execute_command_from_cli_entrypoint


class HelloCommand(Command):
    name = "hello"
    help = "A simple hello command."

    def add_arguments(self, parser):
        parser.add_argument("--name", type=str, default="World", help="Specify the name to greet.")

    def handle(self, **options):
        name = options.get("name")
        print(f"Hello, {name} from Esperoj CLI!")


MAIN_COMMAND = HelloCommand

if __name__ == "__main__":
    execute_command_from_cli_entrypoint(MAIN_COMMAND)
