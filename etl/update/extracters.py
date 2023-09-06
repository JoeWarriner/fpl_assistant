from typing import Any
from abc import ABC, abstractmethod
from pydantic import BaseModel
from etl.update.utils.paths import ProjectPaths
import pandas as pd

class Extracter(ABC):

    @abstractmethod
    def extract() -> list[Any]:
        ...

class APIExtracter(Extracter):
    
    def __init__(self, api_model_class: type[BaseModel], api_data_getter: list[dict[str, Any]]):
        self.api_model_class = api_model_class
        self.api_data_getter = api_data_getter

    def extract(self) -> list[Any]:
        return [self.api_model_class.model_validate(data) for data in self.api_data_getter()]

    def __str__(self):
        return f'API Extractor: {self.api_model_class}'


class DataTableExtracter(Extracter):
    def __init__(self, seasons, filename, pathlib = ProjectPaths):
        self.seasons = seasons
        self.filename = filename
        self.pathlib = pathlib
    
    def extract(self) -> pd.DataFrame:
        season_data_list = []
        for season in self.seasons:
            path = self.pathlib.get_season_data_directory(season) / self.filename
            try:
                season_data = pd.read_csv(path, encoding = 'utf8')
                season_data['season'] = season
                season_data_list.append(season_data)
            except FileNotFoundError:
                print(f'No {self.filename} file found for season: {season}')
        all_season_data = pd.concat(season_data_list)
        return all_season_data
    

    def __str__(self): 
        return f'DataTable Extractor: {self.filename}'
    