from typing import Any
from cache_engine import *
from dataclasses import dataclass

VARTYPE_SINGLE = 1
VARTYPE_LIST = 2

@dataclass
class ComputationObjectReference:
    varname: str
    vartype: int
    data: Any
    co_data: ComputationObjectData

class CoVars:
    co_ref_dict: dict[str, ComputationObjectReference | list[ComputationObjectMetadata]] = {}
    
    @staticmethod
    def add_co_ref(varname: str, obj: Any) -> None:

        # get the co_data based on the 
        co_data = None
        vartype = -1
        if (isinstance(obj, list)):
            co_data = CacheEngine._get_computation_object_data(type(obj[0]))
            vartype = VARTYPE_LIST
        else:
            co_data = CacheEngine._get_computation_object_data(type(obj))
            vartype = VARTYPE_SINGLE

        co_ref = ComputationObjectReference(varname, vartype, obj, co_data)
        CoVars.co_ref_dict[varname] = co_ref

    @staticmethod
    def get_co_ref(varname: str) -> ComputationObjectReference | None:
        if not varname in CoVars.co_ref_dict:
            return None
        
        return CoVars.co_ref_dict[varname]
            
    @staticmethod
    def get_metadata_list_from_ref(ref: ComputationObjectReference):
        if ref.vartype == VARTYPE_LIST:
            metadata_dicts = [ref.co_data.metadata.compute_metadata(obj) for obj in ref.data]
