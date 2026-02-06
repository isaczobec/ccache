

from .computation_object_data import ComputationObjectData

class Void:
    """
    Class specifying that a computation function either
    has no computation object output or inputs.
    """
    pass

class In:
    """
    Class for passing input arguments to a compute function
    """
    def __init__(self, *in_types):
        if len(in_types) == 0 or in_types[0] is Void:
            self.in_types = []
            self.is_void = True
        else:
            self.in_types = in_types

class Out:
    """
    Class for output arguments to a compute function
    """
    def __init__(self, out_type):
        self.out_type = out_type

class ComputationFunction:
    def __init__(
            self, 
            func_name: str,
            func: callable,
            inputs: list[ComputationObjectData], 
            output: ComputationObjectData
            ):
        self.func = func
        self.func_name = func_name
        self.inputs = inputs
        self.output = output

