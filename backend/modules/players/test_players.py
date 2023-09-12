import pytest
from modules.players.db import get_players
from database.test_utils import populated_database

    

def test_get_players_1(populated_database):
    player, = get_players(0, 1)
    assert player.position == 'FWD'
    assert player.first_name == 'Julián'
    assert player.second_name == 'Álvarez'


def test_get_players_2(populated_database):
    player, = get_players(1, 1)
    print(player)
    assert player.position == 'GKP'
    assert player.first_name == 'Alisson'
    assert player.second_name == 'Ramses Becker'

def test_get_players_3(populated_database):
    result = get_players(2,1)
    assert result == []
