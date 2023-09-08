from etl.jobs.base_job import Job
from abc import ABC, abstractmethod

class Loader(Job, ABC):
    expects_input = True


    @abstractmethod
    def run(self):
        pass
        