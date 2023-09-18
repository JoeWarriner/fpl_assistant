from abc import ABC, abstractmethod
from etl.jobs.base_job import Job

class Transformer(Job, ABC):
    """
    Base transformer class.
    """
    expects_input = True

    @abstractmethod
    def run():
        pass
