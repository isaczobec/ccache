from dataclasses import dataclass, field
import inspect
from typing import Callable
from computation_object_data import ComputationObjectData
from computation_object_metadata import ComputationObjectMetadata
from compute_function import In, Out, ComputationFunction
import sqltypes as sqlt
import os
import uuid

from db_manager import DBManager

IS_SAVE_METHOD_FLAG = "_is_save_method"
SAVE_METHOD_NAME = "save_method"
IS_LOAD_METHOD_FLAG = "_is_load_method"
LOAD_METHOD_NAME = "load_method"
METADATA_TUPLE_NAME = "metadata_tuple"

class CacheEngine:

    _data_dir = ".ccache"
    _obj_dir = os.path.join(_data_dir,"objs")
    _db_dir = os.path.join(_data_dir,"db")

    _current_computation_object_type: type = None

    @staticmethod
    def get_current_computation_object_type():
        return CacheEngine._current_computation_object_type

    _computation_object_dict: dict[str, ComputationObjectData] = {}
    """dictonary for storing data for all computation objects."""

    _computation_object_type_to_identifier_dict: dict[type, str] = {}
    """dict that maps a type to its computation object identifier."""

    _computation_function_dict: dict[str, ComputationFunction] = {}
    """dict with for tracking computation functions. Populated in
    `start()`."""

    _computation_function_pre_dict: dict[str, tuple] = {}
    """A dict for keeping track of which functions to turn
    into `ComputationFunction` instances after all functions and
    computation objects have been registered. has the form 
    `func_name : (func, inputs: In, output: Out)`."""

    @staticmethod
    def _register_computation_object(
        cls: type,
        identifier: str,
        metadata: ComputationObjectMetadata = ComputationObjectMetadata(),
        ) -> ComputationObjectData:

        # check that cls is a type and that the identifier is unique 
        if not isinstance(cls, type):
            raise ValueError(f"{cls} must be a type but was {type(cls)}!")

        if identifier in CacheEngine._computation_object_dict:
            raise ValueError(f"the computation object with identifier {identifier} already exists, can not register it again!")

        CacheEngine._current_computation_object_type = cls

        # create the computation object data object
        obj_data = ComputationObjectData(
            metadata=metadata,
            object_identifier=identifier,
            cls=cls,
        )

        # Store the objects data and its identifier in the dicts
        CacheEngine._computation_object_dict[identifier] = obj_data
        CacheEngine._computation_object_type_to_identifier_dict[cls] = identifier

        return obj_data

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
    def _save_object(obj):
        obj_data = CacheEngine._get_computation_object_data(type(obj))
        
        # check that the object has a save method
        save_func = getattr(obj, obj_data.save_method, None)
        if save_func is None:
            raise ValueError(f"the computattion object of type {type(obj)} did not have a save function defined!")
        check_saveload_func_signature(save_func) # verify correct signature

        # uid = str(hash(obj)) # TODO: this is not good
        uid = str(uuid.uuid4())
        path = os.path.join(CacheEngine._obj_dir,uid)
        save_func(path)

        DBManager.insert_computation_object(obj, uid, obj_data)

    @staticmethod
    def _load_object(identifier_or_type: str | type, uid: str) -> any:

        # create a new instance of the object
        obj_data = CacheEngine._get_computation_object_data(identifier_or_type)
        new_obj = object.__new__(obj_data.cls)

        # check that the object has a load method
        load_func = getattr(new_obj, obj_data.load_method, None)
        if load_func is None:
            raise ValueError(f"the computattion object of type {type(new_obj)} did not have a load function defined!")
        check_saveload_func_signature(load_func) # verify correct signature

        # load the object
        path = os.path.join(CacheEngine._obj_dir,uid)
        load_func(path)
        return new_obj

    @staticmethod
    def _initialize():
        DBManager.initialize(CacheEngine._db_dir)
        pass

    @staticmethod
    def start():
        # populate the computation function dict
        for func_name, (func, inputs, output) in CacheEngine._computation_function_pre_dict.items():
            # get all inputs computation object datas
            input_datas = [CacheEngine._get_computation_object_data(in_type) for in_type in inputs.in_types]

            output_data = CacheEngine._get_computation_object_data(output.out_type)

            CacheEngine._computation_function_dict[func_name] = ComputationFunction(
                func_name,
                func,
                input_datas,
                output_data
                )
            
    @staticmethod
    def _register_compute_function(func: callable, inputs: In, output: Out):
        func_name = func.__name__
        if func_name in CacheEngine._computation_function_pre_dict:
            raise KeyError(f"{func_name} already exists as a computation function, cannot create it again!")
        CacheEngine._computation_function_pre_dict[func_name] = (func, inputs, output)

    @staticmethod
    def perform_computation_function(func_name: str, input_objects: list[any], normal_args: tuple):
        comp_func = CacheEngine._computation_function_dict[func_name]

        # if incorrect amount of arguments, throw an exception
        if len(input_objects) != len(comp_func.inputs):
            raise ValueError(f"Wrong amount of arguments for function {func_name}; excpected {len(comp_func.inputs)} but got {len(input_objects)}!")

        # verify that the function was called with correct input arguments
        for idx,inp_object in enumerate(input_objects):
            obj_data = CacheEngine._get_computation_object_data(type(inp_object))

            # if the object datas are not the same, throw an exception
            if obj_data != comp_func.inputs[idx]:
                raise ValueError(f"Wrong input type for function {func_name}; excpected {comp_func.inputs[idx].object_identifier} but got {obj_data.object_identifier}!")

        # call the function
        result_obj = comp_func.func(*input_objects, *normal_args)

        # check that the result is a computation object with correct type
        result_obj_data = CacheEngine._get_computation_object_data(type(result_obj))
        if result_obj_data != comp_func.output:
            raise ValueError(f"The type of the result of the function {func_name} was incorrect; excpected {comp_func.output.object_identifier} but got {result_obj_data.object_identifier}!")

        return result_obj

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
    setattr(func, IS_SAVE_METHOD_FLAG, True)
    return func

def load_method(func):
    # verify that the method signature is correct
    check_saveload_func_signature(func)

    # flag that the method is a save method
    setattr(func, IS_LOAD_METHOD_FLAG, True)
    return func

def metadata_setter(vals: tuple[str]):    
    def func_wrapper(func):
        setattr(func, METADATA_TUPLE_NAME, vals)
        return func
    return func_wrapper

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
        obj_data = CacheEngine._register_computation_object(
            cls=c, 
            identifier=identifier, 
            metadata=metadata)
        
        for name, member in vars(c).items():
            # Check if save and load methods have been defined and set them
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

            # add metadata functions
            metadata_tuple = getattr(member, METADATA_TUPLE_NAME, None)
            if metadata_tuple is not None:
                obj_data.metadata.add_metadata_function(name, metadata_tuple)

        return c
    
    return class_wrapper

def computation_function(inputs: In, output: Out):
    def wrapper(func):
        CacheEngine._register_compute_function(func, inputs, output)
        return func    
    return wrapper

# --- TEST CODE ---

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


u = TestClass2(4)
CacheEngine._save_object(u)

# u = CacheEngine._load_object(TestClass2, "8286946198929")
# print(u.val)

DBManager.print_most_recent_rows(CacheEngine._get_computation_object_data(TestClass2))