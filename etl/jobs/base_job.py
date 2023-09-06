from abc import ABC, abstractmethod

class Job(ABC):

    expects_input: bool

    @abstractmethod
    def run(): 
        ...
