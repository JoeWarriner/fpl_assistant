import pytest
import modules.team_selector.db as db
from modules.team_selector.models import Player
from database.test_utils import populated_database_with_predictions
import pandas as pd

def test_get_player_data(populated_database_with_predictions):
    output = db.get_player_data()
    expected_output = pd.DataFrame(
        columns = ['id', 'current_value', 'predicted_score', 'position_id','team_id'],
        data = [[
            2, 55, 2.7, 1, 2
        ]]
    )
    pd.testing.assert_frame_equal(output, expected_output)

def test_get_players_from_list(populated_database_with_predictions):
    alisson, = db.get_players_from_list([2])
    assert alisson.first_name == 'Alisson'
    assert alisson.second_name == 'Ramses Becker'
    assert alisson.team == 'LIV'
    assert alisson.predicted_score == 2.7
    assert alisson.current_value == 55

