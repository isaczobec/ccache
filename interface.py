import abc
import re
from dataclasses import dataclass
from typing import Callable, Any

ARGTYPE_POS   = 1
ARGTYPE_KW    = 2
ARGTYPE_FLAG  = 3


@dataclass
class ArgInfo:
    arg_name: str
    arg_type: int
    info_str: str
    preprocess_func: Callable[[Any], Any] = lambda x: x


class Command(abc.ABC):
    def __init__(self):
        self.all_args: dict[str, ArgInfo] = {}
        self.pos_args: list[ArgInfo] = []

    # some debugging by chatgpt
    def _parse_args(self, args_str: str) -> tuple[list, dict, set] | None:
        kw_args: dict[str, list[str]] = {}
        flag_args: set[str] = set()

        tokens = args_str.split()

        pos_args: list[str] = []
        i = 0

        # collect positional args (until first flag/kw)
        while i < len(tokens) and not tokens[i].startswith("-"):
            pos_args.append(tokens[i])
            i += 1

        # parse flags and keywords
        while i < len(tokens):
            token = tokens[i]
            name = token.lstrip("-")

            if name not in self.all_args:
                CacheInterface.error(f"Argument '{name}' is not valid for this command")
                return None

            arg_info = self.all_args[name]

            if arg_info.arg_type == ARGTYPE_FLAG:
                flag_args.add(name)
                i += 1

            elif arg_info.arg_type == ARGTYPE_KW:
                i += 1
                values = []
                while i < len(tokens) and not tokens[i].startswith("-"):
                    values.append(tokens[i])
                    i += 1
                kw_args[name] = values

            else:
                CacheInterface.error(f"Argument '{name}' used incorrectly")
                return None

        return pos_args, kw_args, flag_args

    @abc.abstractmethod
    def initialize(self):
        pass

    def register_argument(self, arg: ArgInfo):
        self.all_args[arg.arg_name] = arg
        if arg.arg_type == ARGTYPE_POS:
            self.pos_args.append(arg)

    def execute(self, inp_args: str):
        parsed = self._parse_args(inp_args)
        if not parsed:
            return

        pos_args, kw_args, flag_args = parsed

        proc_pos_args = [
            self.pos_args[i].preprocess_func(a)
            for i, a in enumerate(pos_args)
        ]

        proc_kw_args = {
            k: self.all_args[k].preprocess_func(v)
            for k, v in kw_args.items()
        }

        proc_flag_args = {
            arg for arg in flag_args
        }

        self._execute_logic(proc_pos_args, proc_kw_args, proc_flag_args)

    @abc.abstractmethod
    def _execute_logic(self, pos_args: list, kw_args: dict, flag_args: set):
        pass


class PrintCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "printval",
            ARGTYPE_POS,
            "value to print"
        ))
        self.register_argument(ArgInfo(
            "capital",
            ARGTYPE_FLAG,
            "capitalize output"
        ))
        self.register_argument(ArgInfo(
            "appendafter",
            ARGTYPE_KW,
            "append values"
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        msg = pos_args[0]

        if "capital" in flag_args:
            msg = msg.upper()

        if "appendafter" in kw_args:
            msg += " " + " ".join(kw_args["appendafter"])

        print(msg)


@dataclass
class CommandInfo:
    command_name: str
    command_instance: Command
    command_desc: str


class CacheInterface:
    CURSOR_SYMBOL = "ccache> "
    commands: dict[str, CommandInfo] = {}

    @staticmethod
    def error(message: str):
        print(message)

    @staticmethod
    def register_command(command: CommandInfo):
        CacheInterface.commands[command.command_name] = command
        command.command_instance.initialize()

    @staticmethod
    def repl():
        while True:
            print(CacheInterface.CURSOR_SYMBOL, end="")
            inp = input().strip()

            if not inp:
                continue

            comm, *rest = inp.split()
            args_str = " ".join(rest)

            if comm not in CacheInterface.commands:
                print("Unknown command")
                continue

            try:
                CacheInterface.commands[comm].command_instance.execute(args_str)
            except Exception as e:
                print("Exception:", e)


CacheInterface.register_command(CommandInfo(
    "print",
    PrintCommand(),
    "prints a value"
))

CacheInterface.repl()
