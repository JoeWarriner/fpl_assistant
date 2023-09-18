from typing import Any
from pydantic import BaseModel
from etl.jobs.extractors.base_extractor import Extractor
from etl.utils.logging import log



class APIExtractor(Extractor):
    """
    Extractor class for data from the API. 
    Assumes full API dataset already downloaded to backend/files 
    (This is handled by backend/etl/jobs/extractors/api/api_download )
    """
    
    def __init__(self, api_model_class: type[BaseModel], api_data_getter: list[dict[str, Any]]):
        self.api_model_class = api_model_class
        self.api_data_getter = api_data_getter

    def run(self) -> list[Any]:
        log(f'Extracting: {self.api_model_class}')
        return [self.api_model_class.model_validate(data) for data in self.api_data_getter()]
        

    def __str__(self):
        return f'API Extractor: {self.api_model_class}'

