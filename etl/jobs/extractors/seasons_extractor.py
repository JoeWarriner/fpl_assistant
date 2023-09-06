import datetime
from pathlib import Path
import os
from database.data_access_layer import dal
from etl.jobs.extractors.base_extractor import Extractor

data_path = Path(os.getcwd(), 'etl', 'input', 'data')


PRIOR_SEASONS_LABELS = [

    '2018-19',
    '2019-20',
    '2020-21',
    '2021-22',
    '2022-23',
]


class CreateSeasonsFromList(Extractor):
    def __init__(self, seasons: list[str]) -> None:
        self.seasons = seasons
        self.now_month = datetime.datetime.now().month
        self.now_year = datetime.datetime.now().year


    def run(self):
        seasons_to_add = []
        for season in self.seasons:
            start_year = season[:4]
            is_current = is_current_season_start_year(start_year, self.now_year, self.now_month)
            seasons_to_add.append({
                'season': season,
                'is_current': is_current,
                'start_year': start_year
            }
        )
        return seasons_to_add

    
    
def is_current_season_start_year( season_start_year: int, now_year: int, now_month: int):
    return (
            season_start_year == now_year and now_month >= 8
        ) or (
            season_start_year == now_year - 1 and now_month < 8
    )


class CreateThisSeason(Extractor):
    def __init__(self, today_date: datetime.date):
        self.today_date = today_date

    def run(self):
        if  is_current_season_start_year(self.today_date.year, self.today_date.year, self.today_date.month):
            season_start_year = self.today_date.year
        else:
            season_start_year = self.today_date.year - 1

        season_end_year = season_start_year + 1
        season_str = f'{season_start_year}-{season_end_year % 100}'
        return ({
            'season': season_str,
            'is_current': True,
            'start_year': season_start_year
        })

        


