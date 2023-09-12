from typing import Union
import pulp
import pandas as pd 

from database.data_access_layer import dal
import database.tables as tbl

from sqlalchemy import select


def get_player_data():
    ## Player ID.
    ## Predicted points for next game
    ## Current cost
    ## Position

    pass


def get_optimised_team(player_data: pd.DataFrame) -> list[str]:

    ## Extract relevant variables
    players = player_data['full_name']
    teams = player_data['team']
    distinct_teams = teams.unique()
    total_points = player_data['total_points']
    cost = player_data['now_cost']
    is_gk = player_data['element_type'] == 'GK'
    is_mid = player_data['element_type'] == 'MID' 
    is_def = player_data['element_type'] == 'DEF'
    is_fwd = player_data['element_type'] == 'FWD'

    model = pulp.LpProblem("Integer_Programming_Problem", pulp.LpMaximize)

    ## Objective
    x = pulp.LpVariable.dicts('x', players, cat='Binary')
    model += pulp.lpSum([total_points[i] * x[player] for i, player in enumerate(players)])

    ## Constraints
    model += pulp.lpSum([is_gk[i] * x[player] for i, player in enumerate(players)]) == 1
    model += pulp.lpSum([is_mid[i] * x[player] for i, player in enumerate(players)]) == 4
    model += pulp.lpSum([is_def[i] * x[player] for i, player in enumerate(players)]) == 4
    model += pulp.lpSum([is_fwd[i] * x[player] for i, player in enumerate(players)]) == 2
    model += pulp.lpSum([cost[i] * x[player] for i, player in enumerate(players)]) <= 1000
    for team in distinct_teams:
        model += pulp.lpSum([(teams[i] == team) * x[player] for i, player in enumerate(players)]) <= 3

    ## Solve and return:
    model.solve()
    selected_players  = [player for player in players if x[player].value() == 1]
    return(selected_players)
