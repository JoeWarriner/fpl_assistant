import pytest
import modules.team_selector.optimiser as optimiser
from database.test_utils import populated_database_with_predictions

import pandas as pd

COLUMNS = ['id', 'current_value', 'predicted_score', 'position_id','team_id']

def test_get_player_data(populated_database_with_predictions):
    output = optimiser.get_player_data()
    expected_output = pd.DataFrame(
        columns = COLUMNS,
        data = [[
            2, 55, 2.7, 1, 2
        ]]
    )
    pd.testing.assert_frame_equal(output, expected_output)


@pytest.fixture
def generic_team() -> list[list[int]]:
    '''Get a list of players that forms a legal team'''
    return [
        # id, value, predicted_score, position_id, team id
        [1, 65, 5, 1, 1],
        [2, 65, 5, 1, 1],
        [3, 65, 5, 2, 1],
        [4, 65, 5, 2, 2],
        [5, 65, 5, 2, 2],
        [6, 65, 5, 2, 2],
        [7, 65, 5, 2, 3],
        [8, 65, 5, 3, 3],
        [9, 65, 5, 3, 3],
        [10, 65, 5, 3, 4],
        [11, 65, 5, 3, 4],
        [12, 65, 5, 3, 4],
        [13, 65, 5, 4, 5],
        [14, 65, 5, 4, 5],
        [15, 65, 5, 4, 5],
    ] 


def test_optimised_team__basic_case(generic_team : list[list[int]]):
    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert len(output) == 15


def test_optimised_team__poor_value_player(generic_team: list[list[int]]):
    # Change defender with ID #7 to have low predicted points
    generic_team[6][2] = 1 
    # Add a better value defender at the end
    generic_team.append(
        [16, 65, 5, 2, 3]
    ) 

    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert 7 not in output
    assert 16 in output

def test_optimised_team__high_value_player(generic_team: list[list[int]]):
    # Add a MUCH better value defender at the end
    generic_team.append(
        [16, 65, 100, 2, 3]
    ) 
    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert 16 in output

    
def test_optimised_team__too_expensive_player(generic_team: list[list[int]]):
    # Add a MUCH better value defender at the end, who is too expensive
    generic_team.append(
        [16, 300, 100, 2, 3]
    ) 
    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert 16 not in output


def test_optimised_team__too_many_same_team_players(generic_team: list[list[int]]):
    # Add a series of MUCH better value players, but who are on the same team.
    generic_team.extend([
        [16, 1, 100, 1, 3],
        [17, 1, 100, 2, 3],
        [18, 1, 100, 3, 3],
        [19, 1, 90, 4, 3] # should be excluded.
    ]) 
    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert 19 not in output


def test_optimised_team__too_many_same_position_players(generic_team: list[list[int]]):
    # Add a series of MUCH better value players, but who are all the same position
    generic_team.extend([
        [16, 1, 100, 4, 6],
        [17, 1, 100, 4, 7],
        [18, 1, 100, 4, 8],
        [19, 1, 90, 4, 9] # should be excluded.
    ]) 
    data = pd.DataFrame(columns = COLUMNS, data = generic_team)
    output = optimiser.get_optimised_team(data)
    assert 19 not in output

    
