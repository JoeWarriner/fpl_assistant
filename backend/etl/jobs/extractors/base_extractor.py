from abc import ABC, abstractmethod
from typing import Any
from etl.jobs.base_job import Job



class Extractor(Job, ABC):
    """Base extractor class for use in ETL pipelines. """

    expects_input = False

    @abstractmethod
    def run() -> list[Any]:
        ...
