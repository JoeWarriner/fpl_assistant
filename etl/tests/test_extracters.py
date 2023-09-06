import pytest
import pandas as pd
from pathlib import Path
from etl.jobs.extractors.extracters import DataTableExtractor
from etl.tests.utils import PathsForTests

SEASONS = ['2021-22', '2022-23']



        


def test_df_player_extract():
    extractor = DataTableExtractor(SEASONS, filename='players_raw.csv', pathlib=PathsForTests)
    output = extractor.extract()
    expected_output = pd.DataFrame(
        columns = ['second_name', 'season'],
        data = [
            ['Martínez', '2021-22'],
            ['de Jesus', '2021-22'],
            ['Fernando de Jesus', '2022-23'],
            ['Alexander-Arnold', '2022-23'],
        ])

    # we don't care about order or index values:
    output = output.sort_values(by='second_name').reset_index(drop=True)
    expected_output =expected_output.sort_values(by='second_name').reset_index(drop=True)

    pd.testing.assert_frame_equal(output[['second_name', 'season']], expected_output, check_like=True)


def test_df_gameweek_extract():
    extractor = DataTableExtractor(SEASONS, filename=Path('gws', 'merged_gw.csv'), pathlib=PathsForTests)
    output = extractor.extract()
    expected_output = pd.DataFrame(
        columns = ['GW', 'season'],
        data = [
            [1 , '2021-22'],
            [1 , '2021-22'],
            [2 , '2021-22'],
            [2 , '2021-22'],
            [1 , '2022-23'],
            [1 , '2022-23']
        ]
    )
    print(output)

    output = output.sort_values(by='GW').reset_index(drop=True)
    expected_output =  expected_output.sort_values(by='GW').reset_index(drop=True)
    pd.testing.assert_frame_equal(output[['GW', 'season']], expected_output, check_like=True)

