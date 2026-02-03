from dataclasses import dataclass, field
import inspect
from typing import Callable
import sqlite3
from computation_object_metadata import ComputationObjectMetadata
import sqltypes as sqlt

IS_SAVE_METHOD_FLAG = "_is_save_method"
SAVE_METHOD_NAME = "save_method"
IS_LOAD_METHOD_FLAG = "_is_load_method"
LOAD_METHOD_NAME = "load_method"

@dataclass
class ComputationObjectData:
    cls: type
    metadata: ComputationObjectMetadata = field(default_factory = lambda: ComputationObjectMetadata())
    save_method: str = None
    """Name of the method to save the object"""
    load_method: str = None
    """Name of the method to load the object"""

class CacheEngine:

    _current_computation_object_type: type = None

    @staticmethod
    def get_current_computation_object_type():
        return CacheEngine._current_computation_object_type

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

        CacheEngine._current_computation_object_type = cls

        # create the computation object data object
        objData = ComputationObjectData(
            metadata=metadata,
            cls = cls,
        )

        # Store the objects data and its identifier in the dicts
        CacheEngine._computation_object_dict[identifier] = objData
        CacheEngine._computation_object_type_to_identifier_dict[cls] = identifier

    @staticmethod
    def _get_computation_object_data(identifier_or_type: str | type):
        # find the computation object
        computation_object = None
        if isinstance(identifier_or_type, str):
            computation_object = CacheEngine._computation_object_dict[identifier_or_type]
        elif isinstance(identifier_or_type, type):
            id = CacheEngine._computation_object_type_to_identifier_dict[identifier_or_type]
            computation_object = CacheEngine._computation_object_dict[id]
        else:
            raise ValueError(f"identifier_or_type must be of type str or type; was {type(identifier_or_type)}")

        return computation_object

    @staticmethod
    def _modify_computation_object_data(identifier_or_type: str | type, func: Callable):

        # find the computation object
        computation_object = CacheEngine._get_computation_object_data(identifier_or_type)

        # apply the function to the object
        func(computation_object)

    @staticmethod
    def _save_object(object):
        obj_data = CacheEngine._get_computation_object_data(type(object))
        
        # check that the object has a save method
        save_func = getattr(object, obj_data.save_method, None)
        if save_func is None:
            raise ValueError(f"the computattion object of type {type(object)} did not have a save function defined!")
        check_saveload_func_signature(save_func) # verify correct signature

        filename = str(hash(object))
        save_func(filename)


    @staticmethod
    def _initialize():
        pass

def check_saveload_func_signature(func):
    # verify that the method signature is correct
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    if not len(params) in (1,2):
        raise TypeError(f"A @save_method requires signature of type (self, path: str); got {sig}")

def save_method(func):
    # verify that the method signature is correct
    check_saveload_func_signature(func)
    
    # flag that the method is a save method
    func._is_save_method = True
    return func

def load_method(func):
    # verify that the method signature is correct
    check_saveload_func_signature(func)

    # flag that the method is a save method
    func._is_load_method = True
    return func

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
        
        # Check if save and load methods have been defined and set them
        for name, member in vars(c).items():
            if getattr(member, IS_SAVE_METHOD_FLAG, False):
                CacheEngine._modify_computation_object_data(
                    c, lambda dat, 
                    save_method_name=name : setattr(dat, SAVE_METHOD_NAME, save_method_name)
                    )
            elif getattr(member, IS_LOAD_METHOD_FLAG, False):
                CacheEngine._modify_computation_object_data(
                    c, lambda dat, 
                    load_method_name=name : setattr(dat, LOAD_METHOD_NAME, load_method_name)
                    )

        return c
    
    return class_wrapper

@computation_object("asdssddfa")
class TestClass2:
    def a(self, u):
        print (u)

    @save_method
    def save(self, path):
        with open(path, "w") as file:
            file.write("DSOAHDUSHAUDO")

@computation_object("testclass3")
class TestClass3:
    def a(self, u):
        print (u)

    @save_method
    def save(self, path):
        with open(path, "w") as file:
            file.write("djasiodjsao")


u = TestClass2()
CacheEngine._save_object(u)

u = TestClass3()
CacheEngine._save_object(u)