from typing import Any
from abc import ABC, abstractmethod
from pydantic import BaseModel

class Extracter(ABC):

    @abstractmethod
    def extract():
        ...

class APIExtracter(Extracter):
    
    def __init__(self, api_model_class: type[BaseModel], api_data: list[dict[str, Any]]):
        self.api_model_class = api_model_class
        self.api_data = api_data

    def extract(self):
        return [self.api_model_class.model_validate(data) for data in self.api_data]
         