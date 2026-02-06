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
    uid_objs_dict: dict[str, Any] = {}

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

        # also store references to the objects with uids as keys for fast retrieval
        if vartype == VARTYPE_SINGLE:
            uid = CacheEngine.get_co_hash(obj)
            CoVars.uid_objs_dict[uid] = obj
        elif vartype == VARTYPE_LIST:
            for o in obj:
                uid = CacheEngine.get_co_hash(o)
                CoVars.uid_objs_dict[uid] = o
        
    @staticmethod
    def get_co_ref(varname: str) -> ComputationObjectReference | None:
        if not varname in CoVars.co_ref_dict:
            return None
        
        return CoVars.co_ref_dict[varname]
            
    @staticmethod
    def get_metadata_list_from_ref(ref: ComputationObjectReference):
        if ref.vartype == VARTYPE_LIST:
            return CacheEngine.get_metadatas_for_computation_objects(ref.data)
        elif ref.vartype == VARTYPE_SINGLE:
            return CacheEngine.get_metadatas_for_computation_objects([ref.data])

    @staticmethod
    def get_obj_from_uid(uid: str) -> Any | None:
        """
        Returns the computation object with the given uid, or None if it does not exist.
        """
        if not uid in CoVars.uid_objs_dict:
            return None
        
        return CoVars.uid_objs_dict[uid]