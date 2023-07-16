import pandas as pd
import pulp
import json
import os


def clear_temp_data():
    pass

def get_player_data():
    player_data = pd.read_csv('input/cleaned_players.csv')
    gameweek_data = pd.read_csv('input/gw38.csv')
    gameweek_data = gameweek_data[['name', 'team']]
    player_data['full_name'] = player_data['first_name'] + ' ' + player_data['second_name']
    player_data = player_data.merge(gameweek_data, left_on = 'full_name', right_on = 'name')
    player_data = player_data[player_data['total_points'] > 0].reset_index(drop = True)
    player_data.to_pickle('temp_data/player_data.pkl')


def player_data_to_trade_module():
    player_data = pd.read_pickle('temp_data/player_data.pkl')
    player_data.to_pickle('../trade/data/player_data.pkl')
    

def get_optimised_team():

    player_data = pd.read_pickle('temp_data/player_data.pkl')

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
    selected_players_json = json.dumps(selected_players)
    with open('../team_selector/data/current_best_team.json', 'w') as file:
        file.write(selected_players_json)

if __name__ == '__main__':
    get_player_data()
    player_data_to_trade_module()
    get_optimised_team()

    