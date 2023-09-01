from pathlib import Path
from datetime import date

class ProjectPaths:

    @classmethod
    @property
    def raw_api_data_dir(cls) -> Path:
        path = Path(__file__).parents[2].joinpath('files', 'api_data')
        return path

    @classmethod
    def get_data_path_for_date(cls, date: date) -> Path:
        path = cls.raw_api_data_dir.joinpath(str(date))
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
    


if __name__ == '__main__':
    print(ProjectPaths.get_latest_player_data_path(500))