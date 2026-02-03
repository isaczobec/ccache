from dataclasses import dataclass, field
from typing import Callable
import sqlite3
from computation_object_metadata import ComputationObjectMetadata
import sqltypes as sqlt

@dataclass
class ComputationObjectData:
    cls: type
    metadata: ComputationObjectMetadata = field(default_factory = lambda: ComputationObjectMetadata())
    save_method: Callable[[str], None] = None
    load_method: Callable[[str], None] = None

class CacheEngine:

    _current_computation_object: type = None

    _computation_object_dict: dict[str, ComputationObjectData] = {}
    """dictonary for storing data for all computation objects."""

    _computation_object_type_to_identifier_dict: dict[type, str] = {}
    """dict that maps a type to its computation object identifier."""

    @staticmethod
    def _register_computation_object(
        cls: type,
        identifier: str,
        metadata: ComputationObjectMetadata = ComputationObjectMetadata(),
        ):

        # check that cls is a type and that the identifier is unique 
        if not isinstance(cls, type):
            raise ValueError(f"{cls} must be a type but was {type(cls)}!")

        if identifier in CacheEngine._computation_object_dict:
            raise ValueError(f"the computation object with identifier {identifier} already exists, can not register it again!")

        CacheEngine._current_computation_object = cls

        # create the computation object data object
        objData = ComputationObjectData(
            metadata=metadata,
            cls = cls,
        )

        # Store the objects data and its identifier in the dicts
        CacheEngine._computation_object_dict[identifier] = objData
        CacheEngine._computation_object_dict[cls] = identifier

    @staticmethod
    def _initialize():
        pass

def save_method(func):
    pass

def computation_object(
        identifier: str,
        metadata: ComputationObjectMetadata = ComputationObjectMetadata()
        ):
    """
    Decorator method to mark a class as a Computation Object.
    the metadata objects must have valid SQLlite types.
    
    Sample usage:
    ```python
    @computation_object(
    "LinRegResult",
    metadata=ComputationObjectMetadata(
        n_points = sqlt.BIGINT,
        timestamp = sqlt.DATE,
        )
    )
    class LinRegRes:
    ...
    ```

    :param identifier: The unique identifier for this computation object.
    :type identifier: str
    :param metadata: The metadata assosciated with this object.
    :type metadata: ComputationObjectMetadata
    """

    def class_wrapper(c):
        CacheEngine._register_computation_object(
            cls=c, 
            identifier=identifier, 
            metadata=metadata)
        return c
    
    return class_wrapper

@computation_object(
    "asdsa",
    metadata=ComputationObjectMetadata(
        n_points = sqlt.BIGINT,
        timestamp = sqlt.DATE,
        )
    )
class TestClass:
    def a(self, u):
        print (u)

@computation_object("asdssddfa")
class TestClass2:
    def a(self, u):
        print (u)

@computation_object("asfdsfdsfdsdsa")
class TestClass3:
    def a(self, u):
        print (u)

print(CacheEngine._computation_object_dict)