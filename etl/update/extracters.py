from typing import Any
from abc import ABC, abstractmethod
from pydantic import BaseModel
import sqlalchemy
from etl.update.utils.paths import ProjectPaths
from etl.update.database import dal
import pandas as pd

class Extracter(ABC):

    @abstractmethod
    def extract() -> list[Any]:
        ...

class APIExtracter(Extracter):
    
    def __init__(self, api_model_class: type[BaseModel], api_data: list[dict[str, Any]]):
        self.api_model_class = api_model_class
        self.api_data = api_data

    def extract(self) -> list[Any]:
        return [self.api_model_class.model_validate(data) for data in self.api_data]



class DataTableExtracter(Extracter):
    def __init__(self, seasons, filename, pathlib = ProjectPaths):
        self.seasons = seasons
        self.filename = filename
        self.pathlib = pathlib
    
    def extract(self) -> list[Any]:
        season_data_list = []
        for season in self.seasons:
            path = self.pathlib.get_season_data_directory(season) / self.filename
            print(path)
            try:
                season_data = pd.read_csv(path, encoding = 'latin1')
                season_data['season'] = season
                season_data_list.append(season_data)
            except FileNotFoundError:
                print(f'No {self.filename} file found for season: {season}')
        all_season_data = pd.concat(season_data_list)
        return all_season_data
