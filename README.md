# ccache

created by Isac Zobec, zobec.isac@gmail.com
# ccache

`ccache` is a lightweight computation-object caching framework
with
SQLite-backed metadata storage and an interactive command-line interface.

It allows you to:

- Register **computation object types** with structured metadata
- Persist objects deterministically using hashes
- Define **computation functions** over objects
- Query metadata using SQL
- Interactively select and reuse cached results

---

## Features

- Declarative computation object registration
- Automatic metadata extraction and versioning
- SQLite-backed relational storage
- Deterministic object hashing
- Interactive CLI with history, tab completion, and selection UI
- Zero external dependencies (stdlib only)

---

## Installation (development)

call
```bash
pip install git+https://github.com/izobec/ccache.git
```

to reinstall after updates:
```bash
pip install --upgrade git+https://github.com/izobec/ccache.git
```

# Usage

Before using any methods inluded in this module, you must call `CacheEngine.initialize()`:

## Defining a computation object

```python
from ccache import (
    computation_object,
    save_method,
    load_method,
    metadata_setter,
)
from ccache import sqltypes as sqlt
from ccache import ComputationObjectMetadata


@computation_object(
    "MyNumber",
    metadata=ComputationObjectMetadata(
        squared=sqlt.INT,
        cubed=sqlt.INT,
    ),
)
class MyNumber:
    def __init__(self, value: int):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    @save_method
    def save(self, path: str):
        with open(path, "w") as f:
            f.write(str(self.value))

    @load_method
    def load(self, path: str):
        with open(path, "r") as f:
            self.value = int(f.read())

    @metadata_setter(("squared",))
    def set_squared(self):
        return (self.value ** 2,)

    @metadata_setter(("cubed",))
    def set_cubed(self):
        return (self.value ** 3,)
```

## Saving and Loading Objects

```python
from ccache import CacheEngine

obj = MyNumber(10)
uid = CacheEngine.save_object(obj)

same_obj = CacheEngine.load_object(MyNumber, uid)
```

## Defining computation functions

```python
from ccache import computation_function, In, Out

@computation_function(
    In(MyNumber, MyNumber),
    Out(MyNumber),
)
def add_numbers(a: MyNumber, b: MyNumber, extra: int):
    return MyNumber(a.value + b.value + extra)
```

## Starting and Using the CLI

After defining your computation objects and functions, call `CacheInterface.repl()` to start the CLI.
call the `help` command for help.

### Avaliable commands

```
set – store query results or variables
exec – execute computation functions
sql – run read-only SQL queries
lsc – list computation object types
lsf – list computation functions
lsv – list variables and metadata
help – show help and usage
quit – exit
``` 

Example CLI session:
```
ccache> lsc
MyNumber -> MyNumber

ccache> exec add_numbers -in a b -arg 5 -set result
Saved resulting object with uid 3fa91b...
Stored the result in result!

ccache> lsv -all-metadata
...
```

### SQL queries

You can run SQL queries directly over metadata tables (specified by their identifiers):

```
set res -query "SELECT * FROM :MyNumber WHERE squared > 50;"
```

The `:` prefix resolves to the most recent metadata relation.

## Hashing behavior

You must implement the `__hash__` function on computation objects.
If two objects produce the same hash, they are treated as identical,
and cannot be stored twice in the database.
Override __hash__ carefully.

# License

MIT License. See `LICENSE` file