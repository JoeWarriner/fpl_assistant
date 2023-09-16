from abc import ABC, abstractmethod
from typing import Any

class Job(ABC):

    expects_input: bool

    @abstractmethod
    def run(self): 
        ...
