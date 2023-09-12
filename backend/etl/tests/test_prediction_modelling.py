import pytest

from database.test_utils import populated_database

from etl.modelling.prediction_modelling import ModelPredictions
from etl.imports.initial_import import InitialImport
from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import select

@pytest.fixture
def run_prediction_model(populated_database):
    prediction_job = ModelPredictions()
    prediction_job.window_size = 3
    prediction_job.run()

def test_get_current_players(populated_database):
    prediction_job = ModelPredictions()
    players = prediction_job.get_current_players().all()
    assert len(players) == 2

def test_prediction_output(run_prediction_model):
    """
    Test of basic prediction model - prediction should be mean of previous 3 scores, 
    rounded 2 2DP.
    """
    
    alisson_fixture_1 = dal.session.scalar(
        select(tbl.PlayerFixture).where(tbl.PlayerFixture.id == 1)
        )
    assert alisson_fixture_1.predicted_score == 2.67

    alisson_fixture_2 = dal.session.scalar(
        select(tbl.PlayerFixture).where(tbl.PlayerFixture.id == 2)
        )
    assert alisson_fixture_2.predicted_score == 2.67
