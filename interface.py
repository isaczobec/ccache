# some refactoring help by chatgpt

import abc
import re
from dataclasses import dataclass
import shlex
from typing import Callable, Any
from computation_object_refs import CoVars
from db_manager import DBManager
from cache_engine import *

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

        tokens = shlex.split(args_str)

        pos_args: list[str] = []
        i = 0

        while i < len(tokens) and not tokens[i].startswith("-"):
            pos_args.append(tokens[i])
            i += 1

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

class SetCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "varname",
            ARGTYPE_POS,
            'the varname to store a result in.'
        ))
        self.register_argument(ArgInfo(
            "query",
            ARGTYPE_KW,
            'the query to execute. Escape in quotes ("").'
        ))
        self.register_argument(ArgInfo(
            "var",
            ARGTYPE_KW,
            'To rename a var to another var.'
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        varname = pos_args[0]

        if "query" in kw_args:
            query = kw_args["query"][0]
            ucs = DBManager.get_uids_and_co_ids(query)
            objs = [CacheEngine.load_object(uc[1], uc[0]) for uc in ucs]
            CoVars.add_co_ref(varname, objs)
            return
        
        if "var" in kw_args:
            var = kw_args["var"][0]
            ref = CoVars.get_co_ref(var)
            if ref is None:
                CacheInterface.error(f"The variable {var} did not exist!")
                return
            CoVars.add_co_ref(var, ref.data)

class SetCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "varname",
            ARGTYPE_POS,
            'the varname to store a result in.'
        ))
        self.register_argument(ArgInfo(
            "query",
            ARGTYPE_KW,
            'the query to execute. Escape in quotes ("").'
        ))
        self.register_argument(ArgInfo(
            "var",
            ARGTYPE_KW,
            'To rename a var to another var.'
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        varname = pos_args[0]

        if "query" in kw_args:
            query = kw_args["query"][0]
            ucs = DBManager.get_uids_and_co_ids(query)
            objs = [CacheEngine.load_object(uc[1], uc[0]) for uc in ucs]
            CoVars.add_co_ref(varname, objs)
            return
        
        if "var" in kw_args:
            var = kw_args["var"][0]
            ref = CoVars.get_co_ref(var)
            if ref is None:
                CacheInterface.error(f"The variable {var} did not exist!")
                return
            CoVars.add_co_ref(var, ref.data)


class ExecCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "printval",
            ARGTYPE_POS,
            "value to print"
        ))
        self.register_argument(ArgInfo(
            "in",
            ARGTYPE_KW,
            "Variables to pass to the function"
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
                raise e


CacheInterface.register_command(CommandInfo(
    "print",
    PrintCommand(),
    "prints a value"
))
CacheInterface.register_command(CommandInfo(
    "set",
    SetCommand(),
    "sets a value."
))

CacheEngine._initialize()


@computation_object(
    "Testclass2",
    metadata=ComputationObjectMetadata(
        squaredVal = sqlt.INT,
        cubedVal   = sqlt.INT,
        # name       = sqlt.TEXT,
        # extradata  = sqlt.BOOLEAN,
        # extraextradata  = sqlt.BOOLEAN,
        )
    )
class TestClass2:
    def __init__(self, val):
        self.val = val
    
    @save_method
    def save(self, path):
        with open(path, "w") as file:
            file.write(str(self.val))

    @load_method
    def load(self, path):
        with open(path, "r") as file:
            val = file.read()
            print(f"path: {path}, val: {val}")
            self.val = int(val)

    @metadata_setter(("squaredVal",))
    def set_squaredVal(self):
        sVal = self.val ** 2
        return (sVal, )

    @metadata_setter(("cubedVal",))
    def set_cubedVal(self):
        cVal = self.val ** 3
        return (cVal, )


CacheInterface.repl()
