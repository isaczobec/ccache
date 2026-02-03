from dataclasses import dataclass, field

from computation_object_metadata import ComputationObjectMetadata


@dataclass
class ComputationObjectData:
    cls: type
    object_identifier: str
    metadata: ComputationObjectMetadata = field(default_factory = lambda: ComputationObjectMetadata())
    save_method: str = None
    """Name of the method to save the object"""
    load_method: str = None
    """Name of the method to load the object"""