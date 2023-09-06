import pytest
from datetime import datetime
import etl.seed_db.data_files as tform
from etl.tests.test_seed_db import database
from sqlalchemy.dialects.postgresql import insert
import database.tables as tbl
import pandas as pd
from database.data_access_layer import dal



@pytest.fixture
def insert_season(database):
    dal.session.execute(
        insert(tbl.Season).values({
            tbl.Season.id: 1,
            tbl.Season.start_year: 2021,
            tbl.Season.season: '2021-22',
            tbl.Season.is_current: False
        }
    )
)
    

# def test_convert_season_ids( insert_season):
#     transformer = tform.DataFileTransformer()
#     dataset = pd.DataFrame(
#         {
#             'other_col': ['some data'],
#             'season' : ['2021-22'],  
#         }
#     )
#     output = transformer.convert_season_ids(dataset)
    
#     expected_output = pd.DataFrame(
#         {
#             'other_col': ['some data'],
#             'season' : [1],  
#         }
#     )

#     pd.testing.assert_frame_equal(output, expected_output)


# def test_gameweek_transformer(insert_season):
#     transformer = tform.GameWeekTransformer()
#     dataset = pd.DataFrame(
#         {
#         'kickoff_time':  ['2021-08-14T14:00:00Z'],
#         'GW': [1],
#         'season': ['2021-22']
#         }
#     )
#     output = transformer.convert(dataset)
    
#     expected_output = [
#         {
#             'deadline_time': datetime(2021, 8, 14, 14),
#             'gw_number': 1,
#             'is_current': False,
#             'is_next': False,
#             'finished': True,
#             'is_previous': True,
#             'season': 1
#         }
#     ]

#     assert output == expected_output
    

