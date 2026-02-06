# some refactoring help by chatgpt
# TODO: Replace `print` calls with some logging method on `CacheInterface`

import abc
import re
from dataclasses import dataclass
import shlex
from typing import Callable, Any
from .computation_object_refs import CoVars, VARTYPE_LIST, VARTYPE_SINGLE, ComputationObjectReference
from .db_manager import DBManager
from .cache_engine import *
import curses

import readline  # stdlib on Unix, needs pyreadline on Windows

ARGTYPE_POS   = 1
ARGTYPE_KW    = 2
ARGTYPE_FLAG  = 3




@dataclass
class ArgInfo:
    arg_name: str
    arg_type: int
    info_str: str
    preprocess_func: Callable[[Any], Any] = lambda x: x
    aliases: tuple[str, ...] = ()

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

            # find the argument info from name or aliases
            arg_info = None
            for a in self.all_args.values():
                if name == a.arg_name or name in a.aliases:
                    arg_info = a
                    name = a.arg_name
                    break

            if arg_info is None:
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
            'the varname to store a result in.',
        ))
        self.register_argument(ArgInfo(
            "query",
            ARGTYPE_KW,
            'the query to execute. Escape in quotes ("").',
            aliases=("q",)
        ))
        self.register_argument(ArgInfo(
            "var",
            ARGTYPE_KW,
            'To make the variable point to the same object as another variable.',
            aliases=("v",)
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        varname = pos_args[0]

        if "query" in kw_args:
            query = kw_args["query"][0]

            try:
                ucs = DBManager.get_uids_and_co_ids(query)
            except Exception as e:
                CacheInterface.error(f"Error while executing query: {str(e)}")
                return

            if ucs is None or len(ucs) <= 0:
                print("The query result was empty!")
                return
            
            # get and print the query res
            co_data = CacheEngine._get_computation_object_data(ucs[0][1])
            uids = [uc[0] for uc in ucs]
            rows = DBManager.get_rows_for_obj_uids(uids, co_data)
            string_rep = DBManager.get_string_rep_for_query_res(rows)
            print(string_rep)

            # load the objects
            objs = [CacheEngine.load_object(uc[1], uc[0]) for uc in ucs]
            
            # convert to list to single object if there is only one object
            if len(objs) == 1: objs = objs[0]

            CoVars.add_co_ref(varname, objs)

            print(f"Stored the result in the variable {varname}!")
            return
        
        if "var" in kw_args:
            var = kw_args["var"][0]
            ref = CoVars.get_co_ref(var)
            if ref is None:
                CacheInterface.error(f"The variable {var} did not exist!")
                return
            print(f"Made {varname} point to the value in {var}!")
            CoVars.add_co_ref(var, ref.data)


class ExecCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "computation function",
            ARGTYPE_POS,
            "Which computation function to perform"
        ))
        self.register_argument(ArgInfo(
            "in",
            ARGTYPE_KW,
            "Computation object references to pass to the function."
        ))
        self.register_argument(ArgInfo(
            "arg",
            ARGTYPE_KW,
            "Regular arguments to pass to the function.",
            aliases=("a", "args")
        ))
        self.register_argument(ArgInfo(
            "set",
            ARGTYPE_KW,
            "Stores the result as a variable with the given name."
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        func_name = pos_args[0]

        # the list where final computation object arguments will be stored
        input_computation_objects = []

        # the list where normal args will be stored
        normal_args = []

        # find the inputs
        if "in" in kw_args:
            for varname in kw_args["in"]:
                ref = CoVars.get_co_ref(varname)
                # append the data the reference is pointing to
                if ref is None:
                    CacheInterface.error(f"The variable {varname} does not exist!")
                    return
                
                if ref.vartype == VARTYPE_LIST:
                    sel_uid = CacheInterface.select_uid_from_query_res(
                        CacheEngine.get_metadatas_for_computation_objects(ref.data),
                        f"Select an object from variable {varname} to pass to {func_name}"
                    )
                    if sel_uid is None:
                        CacheInterface.error(f"No selection made for variable {varname}!")
                        return
                    obj = CoVars.get_obj_from_uid(sel_uid)
                    if obj is None:
                        CacheInterface.error(f"Could not find object with uid {sel_uid} from variable {varname}!")
                        return
                    input_computation_objects.append(obj)

                elif ref.vartype == VARTYPE_SINGLE:
                    input_computation_objects.append(ref.data)

        # check that the correct amount of args have been passed
        # to the computation function.
        # Type checking the args happens in `perform_computation_function`.
        input_datas = CacheEngine.get_computation_function_input_datas(func_name)
        if len(input_computation_objects) < len(input_datas):
            for i in range(len(input_computation_objects), len(input_datas)):
                # prompt the user to select the input data
                input_data = input_datas[i]
                uid = CacheInterface.select_uid_from_query_res(
                    DBManager.get_all_rows_for_co_id(input_data.object_identifier),
                    f"Select {input_data.object_identifier} to pass as arg {i}:")
                
                inp_obj = CacheEngine.load_object(input_data.object_identifier, uid)
                input_computation_objects.append(inp_obj)

        # find the args
        if "arg" in kw_args:
            normal_args = kw_args["arg"]

        res_obj = None
        try:
            res_obj = CacheEngine.perform_computation_function(func_name, input_computation_objects, normal_args)
        except Exception as e:
            CacheInterface.error(f"Error while performing {func_name}: {e}")
            raise e
    
        if res_obj is None: return
        
        uid = CacheEngine.save_object(res_obj)
        print(f"Saved resulting object with uid {uid[0:9]}...")

        if "set" in kw_args:
            varname = kw_args["set"][0]
            CoVars.add_co_ref(varname, res_obj)
            print(f"Stored the result in {varname}!")

class SqlCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "query",
            ARGTYPE_POS,
            "SQL query to execute. INSERTing rows is prohibited. Use quotes if needed.",
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        query = pos_args[0].strip()

        if query.lower().startswith("insert"):
            CacheInterface.error("INSERT queries are not allowed.")
            return

        try:
            res = DBManager.query(query)
        except Exception as e:
            CacheInterface.error(f"SQL error: {e}")
            return

        print(DBManager.get_string_rep_for_query_res(res))

class ListComputationObjectsCommand(Command):
    def initialize(self):
        pass

    def _execute_logic(self, pos_args, kw_args, flag_args):
        if not CacheEngine._computation_object_dict:
            print("[No computation objects registered]")
            return

        print("Computation Objects:")
        for identifier, data in CacheEngine._computation_object_dict.items():
            print(f"  {identifier:<20} -> {data.cls.__name__}")

class ListComputationFunctionsCommand(Command):
    def initialize(self):
        pass

    def _execute_logic(self, pos_args, kw_args, flag_args):
        if not CacheEngine._computation_function_dict:
            print("[No computation functions registered]")
            return

        print("Computation Functions:")

        for name, comp_func in CacheEngine._computation_function_dict.items():
            sig = inspect.signature(comp_func.func)

            input_types = [
                d.object_identifier for d in comp_func.inputs
            ]

            normal_params = list(sig.parameters.values())[len(input_types):]
            normal_args = ", ".join(
                f"{p.name}: {p.annotation.__name__ if p.annotation is not inspect._empty else 'Any'}"
                for p in normal_params
            )

            out_type = comp_func.output.object_identifier

            print(f"\n{name}")
            print(f"  inputs : {', '.join(input_types)}")
            print(f"  args   : ({normal_args})")
            print(f"  output : {out_type}")

class HelpCommand(Command):

    def initialize(self):
        self.register_argument(ArgInfo(
            "command",
            ARGTYPE_KW,
            "Show detailed help for a single command",
            aliases=("c", "cmd")
        ))

        self.register_argument(ArgInfo(
            "all-args",
            ARGTYPE_FLAG,
            "Show arguments for all commands",
            aliases=("a", "args")
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        commands = CacheInterface.commands

        if not commands:
            print("[No commands registered]")
            return

        # ---- ALL ARGS FIRST ----
        print("use -command <commandname> for more details on a specific command")
        print("use -all-args to show all arguments for all commands\n")
        if "all-args" in flag_args:
            print("=== Command Arguments ===")
            for name, cmd_info in commands.items():
                print(f"{name}: {cmd_info.command_desc}")
                self._print_command_args(name, cmd_info)
                print()

            print("=== Commands ===")
            return

        # ---- SINGLE COMMAND ----
        if "command" in kw_args:
            cmd_name = kw_args["command"][0]
            if cmd_name not in commands:
                CacheInterface.error(f"Unknown command '{cmd_name}'")
                return

            self._print_command_full(cmd_name, commands[cmd_name])
            return

        # ---- DEFAULT: command list ----
        for name, cmd_info in commands.items():
            print(f"{name:<15} {cmd_info.command_desc}")

    def _print_command_full(self, name, cmd_info):
        print(f"{name}")
        print(f"  {cmd_info.command_desc}")
        self._print_command_args(name, cmd_info)

    def _print_command_args(self, name, cmd_info):
        cmd = CacheInterface.commands[name].command_instance

        if not cmd.all_args or len(cmd.all_args) <= 0:
            print(f"{name}: [no arguments]")
            return

        print(f"usage {name}:")
        usage_str = f"  {name}"
        for arg in cmd.pos_args:
            usage_str += f" <{arg.arg_name}>"
        for cmd_arg in cmd.all_args.values():
            if cmd_arg.arg_type == ARGTYPE_KW:
                usage_str += f" [-{cmd_arg.arg_name} [...]]"
            elif cmd_arg.arg_type == ARGTYPE_FLAG:
                usage_str += f" [-{cmd_arg.arg_name}]"

        print(usage_str)

        for arg_name, arg in cmd.all_args.items():
            if arg.arg_type == ARGTYPE_POS:
                kind = "positional"
            elif arg.arg_type == ARGTYPE_KW:
                kind = "keyword"
            elif arg.arg_type == ARGTYPE_FLAG:
                kind = "flag"
            else:
                kind = "unknown"

            print(f"    {arg_name:<12} ({kind}) | {arg.info_str}")
            if arg.aliases:
                alias_str = f"(aliases: {', '.join(arg.aliases)})"
                print(f"      {'':<12} {alias_str}")

class ClearCommand(Command):
    def initialize(self):
        pass

    def _execute_logic(self, pos_args, kw_args, flag_args):
        import os
        os.system("cls" if os.name == "nt" else "clear")

class LsVarsCommand(Command):
    def initialize(self):
        self.register_argument(ArgInfo(
            "varname",
            ARGTYPE_KW,
            "Show metadata for the given variable name",
            aliases=("v",)
        ))

        self.register_argument(ArgInfo(
            "all-metadata",
            ARGTYPE_FLAG,
            "Show all metadata rows for the variable(s)",
            aliases=("a",)
        ))

    def _execute_logic(self, pos_args, kw_args, flag_args):
        if not CoVars.co_ref_dict:
            print("[No variables defined]")
            return

        # list all variables
        print("Variables:")
        for varname, ref in CoVars.co_ref_dict.items():
            vartype_str = "List" if ref.vartype == VARTYPE_LIST else "Single"
            type_name = ref.co_data.cls.__name__
            print(f"  {varname:<15} ({vartype_str}) -> {type_name}")

        # helper to print metadata nicely using get_string_rep_for_query_res
        def print_metadata(ref: ComputationObjectReference):
            # get uids
            if ref.vartype == VARTYPE_SINGLE:
                objs = [ref.data]
            else:
                objs = ref.data

            uids = [CacheEngine.get_co_hash(o) for o in objs]
            rows = DBManager.get_rows_for_obj_uids(uids, ref.co_data)
            print(DBManager.get_string_rep_for_query_res(rows))

        # if -varname is passed, show metadata
        if "varname" in kw_args:
            var_to_show = kw_args["varname"][0]
            ref = CoVars.get_co_ref(var_to_show)
            if ref is None:
                print(f"Variable '{var_to_show}' does not exist!")
                return
            
            print(f"\nMetadata for '{var_to_show}':")
            print_metadata(ref)

        # if -all-metadata flag is set, show all rows for all variables
        elif "all-metadata" in flag_args:
            for varname, ref in CoVars.co_ref_dict.items():
                print(f"\nMetadata for '{varname}':")
                print_metadata(ref)


class QuitCommand(Command):
    def initialize(self):
        pass

    def _execute_logic(self, pos_args, kw_args, flag_args):
        print("Exiting...")
        CacheInterface.shouldExit = True

@dataclass
class CommandInfo:
    command_name: str
    command_instance: Command
    command_desc: str

class Ansi:
    RESET = "\033[0m"
    BOLD = "\033[1m"

    CYAN = "\033[36m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"


# initialize command line history file (ChatGPT generated)
import os
histfile = os.path.expanduser("~/.ccache_history")
try:
    readline.read_history_file(histfile)
except FileNotFoundError:
    pass

import atexit
atexit.register(readline.write_history_file, histfile)

# customize tab completion
readline.parse_and_bind("tab: complete")

class CacheInterface:
    CURSOR_SYMBOL = f"{Ansi.BOLD}{Ansi.CYAN}ccache>{Ansi.RESET} "
    commands: dict[str, CommandInfo] = {}

    shouldExit = False

    @staticmethod
    def error(message: str):
        print(message)

    @staticmethod
    def register_command(command: CommandInfo):
        CacheInterface.commands[command.command_name] = command
        command.command_instance.initialize()

    @staticmethod
    def repl():
        CacheEngine.start()
        while True:
            inp = input(CacheInterface.CURSOR_SYMBOL).strip()

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
            
            if CacheInterface.shouldExit:
                break
            
    @staticmethod
    def select_uid_from_query_res(query_res: Any, prompt: str) -> str | None:
        """
        Interactive selector for query results.
        Uses arrow keys to select a row and Enter to confirm.
        Returns the uid of the selected row, or None if cancelled.
        """

        if not query_res:
            return None

        columns = list(query_res[0].keys())
        uid_col = "uid"

        if uid_col not in columns:
            raise ValueError("Query result does not contain a 'uid' column")

        table_lines = DBManager.get_string_rep_for_query_res(query_res).splitlines()
        header_lines = table_lines[:2]
        data_lines = table_lines[2:]

        def curses_main(stdscr):
            curses.curs_set(0)
            stdscr.keypad(True)

            # ---- colors ----
            if curses.has_colors():
                curses.start_color()
                curses.use_default_colors()

                curses.init_pair(1, curses.COLOR_CYAN, -1)     # prompt
                curses.init_pair(2, curses.COLOR_YELLOW, -1)   # header
                curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)  # selected row
                curses.init_pair(4, curses.COLOR_WHITE, -1)    # normal rows
                curses.init_pair(5, curses.COLOR_RED, -1)      # quit hint

            selected = 0
            offset = 0

            while True:
                stdscr.clear()
                height, width = stdscr.getmaxyx()

                # ---- prompt line ----
                prompt_line = f"{prompt}   (Esc/q to quit, Enter to select)"
                stdscr.attron(curses.color_pair(1) | curses.A_BOLD)
                stdscr.addstr(0, 0, prompt_line[:width])
                stdscr.attroff(curses.color_pair(1) | curses.A_BOLD)

                # ---- header ----
                for i, line in enumerate(header_lines):
                    stdscr.attron(curses.color_pair(2) | curses.A_BOLD)
                    stdscr.addstr(1 + i, 0, line[:width])
                    stdscr.attroff(curses.color_pair(2) | curses.A_BOLD)

                table_top = 1 + len(header_lines)
                visible_rows = height - table_top - 1

                start = offset
                end = min(offset + visible_rows, len(data_lines))

                # ---- data rows ----
                for i, line in enumerate(data_lines[start:end]):
                    row_idx = start + i
                    y = table_top + i

                    if row_idx == selected:
                        stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
                        stdscr.addstr(y, 0, line[:width])
                        stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)
                    else:
                        stdscr.attron(curses.color_pair(4))
                        stdscr.addstr(y, 0, line[:width])
                        stdscr.attroff(curses.color_pair(4))

                stdscr.refresh()

                key = stdscr.getch()

                if key in (curses.KEY_UP, ord("k")):
                    if selected > 0:
                        selected -= 1
                        if selected < offset:
                            offset -= 1

                elif key in (curses.KEY_DOWN, ord("j")):
                    if selected < len(data_lines) - 1:
                        selected += 1
                        if selected >= offset + visible_rows:
                            offset += 1

                elif key in (curses.KEY_ENTER, 10, 13):
                    return query_res[selected][uid_col]

                elif key in (27, ord("q")):
                    return None

        return curses.wrapper(curses_main)

CacheInterface.register_command(CommandInfo(
    "set",
    SetCommand(),
    "sets a value."
))
CacheInterface.register_command(CommandInfo(
    "exec",
    ExecCommand(),
    "executes a computation function."
))
CacheInterface.register_command(CommandInfo(
    "sql",
    SqlCommand(),
    "execute a read-only SQL query and print the result"
))

CacheInterface.register_command(CommandInfo(
    "lsc",
    ListComputationObjectsCommand(),
    "list all computation object types"
))

CacheInterface.register_command(CommandInfo(
    "lsf",
    ListComputationFunctionsCommand(),
    "list all computation functions and their signatures"
))

CacheInterface.register_command(CommandInfo(
    "help",
    HelpCommand(),
    "show available commands and usage information"
))

CacheInterface.register_command(CommandInfo(
    "clear",
    ClearCommand(),
    "clear the screen"
))

CacheInterface.register_command(CommandInfo(
    "lsv",
    LsVarsCommand(),
    "List all variables, optionally show metadata"
))

CacheInterface.register_command(CommandInfo(
    "quit",
    QuitCommand(),
    "exit the program"
))


if __name__ == "__main__":

    CacheEngine.initialize()

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

        def __hash__(self):
            return hash(self.val)
        
        @save_method
        def save(self, path):
            with open(path, "w") as file:
                file.write(str(self.val))

        @load_method
        def load(self, path):
            with open(path, "r") as file:
                val = file.read()
                self.val = int(val)

        @metadata_setter(("squaredVal",))
        def set_squaredVal(self):
            sVal = self.val ** 1
            return (sVal, )

        @metadata_setter(("cubedVal",))
        def set_cubedVal(self):
            cVal = self.val ** 3
            return (cVal, )


    @computation_function(
            In(TestClass2, TestClass2), 
            Out(TestClass2)
    )
    def test_func(a: TestClass2, b: TestClass2, addExtra: int):
        c = TestClass2(a.val + b.val + addExtra)
        return c

    @computation_function(
            In(Void), 
            Out(TestClass2)
    )
    def other():
        print("HELLO FROM THE OTHER FUNCTION!")
        return TestClass2(42)

    for i in range(3):
        u = TestClass2(i+10)
        CacheEngine.save_object(u)

    CacheInterface.repl()
