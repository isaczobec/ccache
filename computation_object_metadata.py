import sqltypes as st

class ComputationObjectMetadata():
    def __init__(self, **kwargs):
        self.metadata_items = {}
        
        for varname, typename in kwargs.items():
            if not st.typename_islegal(typename):
                raise ValueError(f"{typename} is not a valid SQLlite typename for variable {varname}!")
            self.metadata_items[varname] = typename