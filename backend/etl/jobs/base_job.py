from abc import ABC, abstractmethod

class Job(ABC):
    """
    Base Job class.
    Specificies interface for all data import tasks to implement.
    """

    expects_input: bool

    @abstractmethod
    def run(self): 
        ...
