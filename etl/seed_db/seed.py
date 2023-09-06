import datetime
from pathlib import Path
import os
from database.data_access_layer import dal

data_path = Path(os.getcwd(), 'etl', 'input', 'data')


PRIOR_SEASONS_LABELS = [

    '2018-19',
    '2019-20',
    '2020-21',
    '2021-22',
    '2022-23',
]


class CreateSeasons:
    def __init__(self, seasons: list[str]) -> None:
        self.seasons = seasons
        self.now_month = datetime.datetime.now().month
        self.now_year = datetime.datetime.now().year


    def extract(self):
        seasons_to_add = []
        for season in self.seasons:
            start_year = season[:4]
            is_current = self.season_is_current(start_year)
            seasons_to_add.append({
                'season': season,
                'is_current': is_current,
                'start_year': start_year
            }
        )
        return seasons_to_add

    
    def season_is_current(self, season_start_year: int):
        return (
                season_start_year == self.now_year and self.now_month >= 8
            ) or (
                season_start_year == self.now_year - 1 and self.now_month < 8
        )


