import pytest
import pandas as pd
from pathlib import Path
from etl.update.extracters import DataTableExtracter


class TestPathLib: 
    @classmethod
    def get_season_data_directory(cls, season):
        return Path(__file__).parents[0].joinpath('test_data', 'test_tables', season)


def test_df_extract():
    extractor = DataTableExtracter(['2016-17', '2017-18'], filename='players.csv', pathlib=TestPathLib)
    output = extractor.extract()
    expected_output = pd.DataFrame(
        columns = ['name', 'season'],
        data = [
            ['Diego Maradonna', '2016-17'],
            ['Pele', '2016-17'],
            ['Johan Cruyff', '2017-18'],
            ['Stanley Matthews', '2017-18'],
        ])

    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected_output.reset_index(drop=True))