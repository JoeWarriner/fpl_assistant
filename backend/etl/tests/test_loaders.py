import pytest

import etl.jobs.loaders.loaders as loaders
from database.test_utils import populated_database

from sqlalchemy import select

import database.tables as tbl
from database.data_access_layer import DataAccessLayer

dal = DataAccessLayer()

def test_update_predictions(populated_database):
    """Test new predictions will load to database"""
    test_predictions = {
        1: 7,
        2: 3
    }

    loader = loaders.UpdatePredictions()
    loader.run(test_predictions) 

    fixture_1 = dal.session.scalars(
        select(tbl.PlayerFixture).where(
            tbl.PlayerFixture.id == 1
        )).one()
    assert fixture_1.predicted_score == 7

    fixture_2 = dal.session.scalars(
        select(tbl.PlayerFixture).where(
            tbl.PlayerFixture.id == 2
        )).one()
    assert fixture_2.predicted_score == 3
    

    


