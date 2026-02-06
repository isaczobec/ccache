
from .cache_engine import *

CacheEngine.initialize()

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
            self.val = int(val)

    @metadata_setter(("squaredVal",))
    def set_squaredVal(self):
        sVal = self.val ** 2
        return (sVal, )

    @metadata_setter(("cubedVal",))
    def set_cubedVal(self):
        cVal = self.val ** 3
        return (cVal, )


# u = TestClass2(4)
# CacheEngine.save_object(u)

# u = CacheEngine._load_object(TestClass2, "8286946198929")
# print(u.val)

# DBManager.print_most_recent_rows(CacheEngine._get_computation_object_data(TestClass2))

uc = DBManager.get_uids_and_co_ids("SELECT * FROM :Testclass2 LIMIT 1;")
print(uc)
obj: TestClass2 = CacheEngine.load_object(uc[0][1], uc[0][0])
print("obj val:",obj.val)