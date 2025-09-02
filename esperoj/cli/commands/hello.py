from esperoj.cli.main import Command


class HelloCommand(Command):
    name = "hello"
    help = "A simple hello command."

    def add_arguments(self, parser):
        parser.add_argument("--name", type=str, default="World", help="Specify the name to greet.")

    def handle(self, **options):
        name = options.get("name")
        print(f"Hello, {name} from Esperoj CLI!")
