from abc import ABC

class Loader(ABC):
    def run(self):
        raise NotImplementedError()
        