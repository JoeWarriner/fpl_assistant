from pathlib import Path
from datetime import date
import os

class ProjectPaths:

    @classmethod
    @property
    def project_directory(cls) -> Path:
        return Path(__file__).parents[2]
    
    @classmethod
    @property
    def files_directory(cls) -> Path:
        return cls.project_directory.joinpath('files')
    

    @classmethod
    @property
    def api_data_directory(cls) -> Path:
        return cls.files_directory.joinpath('api_data')
    
    @classmethod
    @property
    def table_data_directory(cls) -> Path:
        return cls.files_directory.joinpath('data_tables')

    @classmethod
    def get_data_path_for_date(cls, date: date) -> Path:
        path = cls.api_data_directory.joinpath(str(date))
        return path
        
    @classmethod
    @property
    def latest_daily_data_dir(cls) -> Path:
        today_date = date.today()
        return cls.get_data_path_for_date(today_date)

    @classmethod
    @property
    def latest_main_data(cls) -> Path:
        return cls.latest_daily_data_dir.joinpath('main.json')

    @classmethod
    @property
    def latest_fixture_data(cls) -> Path:
        return cls.latest_daily_data_dir.joinpath('fixtures.json')

    @classmethod
    @property
    def latest_player_data_dir(cls) -> Path:
        return cls.latest_daily_data_dir.joinpath('player_details')
    
    @classmethod
    def get_latest_player_data_path(cls, fpl_id: int , name: str):
        return cls.latest_player_data_dir.joinpath(f'{fpl_id}_{name}.json')
    
    @classmethod
    def get_all_player_data_paths(cls) -> tuple[int, Path]:
        return {
            (int(file.split('_')[0]), Path(cls.latest_player_data_dir, file))
            for file in os.listdir(cls.latest_player_data_dir) 
            if file.split('.')[-1] == 'json'
        }
    
    @classmethod
    def get_season_data_directory(cls, season: str) -> list[Path]:
        return cls.table_data_directory.joinpath(season)





        
if __name__ == '__main__':
    print(ProjectPaths.get_latest_player_data_path(500))