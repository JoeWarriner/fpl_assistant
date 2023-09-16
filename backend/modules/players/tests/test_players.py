import pytest
from modules.players.db import get_players
from database.test_utils import populated_database_with_predictions, populated_database

    

def test_get_players_1(populated_database_with_predictions):
    alisson, = get_players(0, 1)
    assert alisson.first_name == 'Alisson'
    assert alisson.second_name == 'Ramses Becker'
    assert alisson.team == 'LIV'
    assert alisson.predicted_score == 2.7
    assert alisson.current_value == 55



def test_get_players_1(populated_database):
    alisson, = get_players(0, 1)
    assert alisson.predicted_score == None


def test_get_players_2(populated_database_with_predictions):
    result = get_players(1, 1)
    assert result == []

