def main():
    from .cache_engine import CacheEngine
    from .interface import CacheInterface

    CacheEngine.initialize()
    CacheInterface.repl()
