import pytest

from database.test_utils import populated_database

from etl.modelling.basic_model import SimpleRollingMeanPrediction
from etl.imports.initial_import import InitialImport
from database.data_access_layer import DataAccessLayer
import database.tables as tbl
from datetime import datetime
from sqlalchemy import select

dal = DataAccessLayer()


def test_get_current_players(populated_database):
    prediction_job = SimpleRollingMeanPrediction()
    players = prediction_job.get_current_players()
    assert len(players) == 2

def test_get_player_recent_performances(populated_database):
    prediction_job = SimpleRollingMeanPrediction()
    prediction_job.window_size = 2
    alisson = dal.session.scalar(select(tbl.Player).where(tbl.Player.id == 2))
    performances = prediction_job.get_player_recent_peformances(alisson)
    assert len(performances) == 2
    assert performances[0] == 3
    assert performances[1] == 3

def test_calculate_mean():
    performances = [2,3,3]
    prediction_job = SimpleRollingMeanPrediction()
    mean = prediction_job.calculate_mean(performances)
    assert f'{mean:.2f}' == '2.67'

def test_get_player_future_fixture_ids(populated_database):
    prediction_job = SimpleRollingMeanPrediction()
    prediction_job.today_date = datetime(2022, 8, 1)
    alisson = dal.session.scalar(select(tbl.Player).where(tbl.Player.id == 2))
    player_fixtures = prediction_job.get_player_future_fixtures(alisson )
    assert len(player_fixtures) == 2
    

def test_prediction_output(populated_database):
    """
    Test of basic prediction model - prediction should be mean of previous 3 scores, 
    rounded 2 2DP.
    """
    
    prediction_job = SimpleRollingMeanPrediction()
    prediction_job.window_size = 3
    prediction_job.today_date = datetime(2022, 8, 1)
    outputs = prediction_job.run()

    # Convert to string to avoid floating point issues
    assert f'{outputs[1]:.2f}' == '2.67'
    assert f'{outputs[2]:.2f}' == '2.67'