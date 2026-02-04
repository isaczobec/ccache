import abc
from cache_engine import *
from db_manager import *

@abc
class Command:
    def __init__(self):
        pass

    

    def set_args(self, pos_args: list, kw_args: dict, flag_args: set):
        self.pos_args = pos_args
        self.kw_args = kw_args
        self.flag_args = flag_args

    @abc.abstractmethod
    def execute(self):
        pass

class CacheInterface:
