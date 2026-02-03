

from computation_object_data import ComputationObjectData


class In:
    """
    Class for passing input arguments to a compute function
    """
    def __init__(self, *in_types):
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

