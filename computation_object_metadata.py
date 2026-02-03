import sqltypes as st

class ComputationObjectMetadata():
    def __init__(self, **kwargs):
        self.metadata_items: dict[str, str] = {}

        self._metadata_functions: dict[str, tuple[str]] = {}
        """A dict containing function names as keys,
        and tuples of the names of the values they 
        return as values."""
        
        for varname, typename in kwargs.items():
            if not st.typename_islegal(typename):
                raise ValueError(f"{typename} is not a valid SQLlite typename for variable {varname}!")
            self.metadata_items[varname] = typename

    def add_metadata_function(self, funcname: str, varnames: tuple[str]):

        # check that all variables exist
        for var in varnames:
            if not var in self.metadata_items:
                raise KeyError(f"The variable {var} did not exist on the Computation Object!")

        # add them
        self._metadata_functions[funcname] = varnames

    def compute_metadata(self, obj) -> dict:

        vars = {}

        for funcname, varnames in self._metadata_functions.items():
            metadata_func = getattr(obj, funcname, None)
            if metadata_func is None:
                raise NameError(f"The object of type {type(obj)} did not have a function with the name {funcname}!")

            vals = metadata_func() # compute the metadata
            for var, val in zip(varnames, vals, strict = True):
                vars[var] = val

        return vars
                