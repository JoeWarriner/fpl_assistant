from etl.jobs.extractors.base_extractor import Extractor
from etl.utils.paths import ProjectPaths
import pandas as pd

class DataTableExtractor(Extractor):
    def __init__(self, seasons, filename, pathlib = ProjectPaths):
        self.seasons = seasons
        self.filename = filename
        self.pathlib = pathlib
    
    def run(self) -> pd.DataFrame:
        season_data_list = []
        for season in self.seasons:
            path = self.pathlib.get_season_data_directory(season) / self.filename
            try:
                season_data = pd.read_csv(path, encoding = 'utf-8')
                season_data['season'] = season
                season_data_list.append(season_data)
            except UnicodeDecodeError:
                season_data = pd.read_csv(path, encoding='latin1')
                season_data['season'] = season
                season_data_list.append(season_data)
            except FileNotFoundError:
                print(f'No {self.filename} file found for season: {season}')
            
            
        all_season_data = pd.concat(season_data_list)
        return all_season_data
    

    def __str__(self): 
        return f'DataTable Extractor: {self.filename}'
    