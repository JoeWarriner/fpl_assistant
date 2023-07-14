from typing import Union
import pulp
import pandas as pd 

## Data cleaning

def get_clean_data() -> pd.DataFrame:
    player_data = pd.read_csv('data/2021-22/cleaned_players.csv')
    gameweek_data = pd.read_csv('data/2021-22/gw38.csv')
    gameweek_data = gameweek_data[['name', 'team']]
    player_data['full_name'] = player_data['first_name'] + ' ' + player_data['second_name']
    player_data = player_data.merge(gameweek_data, left_on = 'full_name', right_on = 'name')
    player_data = player_data[player_data['total_points'] > 0].reset_index(drop = True)
    return player_data

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


def get_best_single_trade(team: list[str], player_data: pd.DataFrame) -> dict[str,  Union[str, int]]:

    # Get data for team.
    team_data = player_data[player_data['name'].apply(lambda x: True if x in team else False)]
    teams = team_data.groupby('team').size()
    unavailable_teams = list(teams[teams == 3].index)
    team_value = team_data['now_cost'].sum()
    team_points = team_data['total_points'].sum()
    spare_cash = 1000 - team_value



    best_trade = {
        'player_out': 'None',
        'player_in': 'None',
        'points_differential': 0
    }

    for i in team_data.index:

        # Get available money for this trade

        trade_money_available = team_data['now_cost'][i] + spare_cash

        # Get available players for this trade:     
        available_players = player_data[player_data['element_type'] == team_data['element_type'][i]] 
        available_players = available_players[available_players['now_cost'] <= trade_money_available]
        this_trade_unavailable_teams = [team for team in unavailable_teams if team != team_data['team'][i]]
        available_players = available_players[available_players['team'].apply(lambda x: False if x in this_trade_unavailable_teams else True)]
        available_players = available_players[available_players['name'].apply(lambda x: False if x in team else True)]
        available_players_sorted = available_players.sort_values('total_points', ascending= False).reset_index(drop= True)

        # Find best trade and return points differential
        points_differential = available_players_sorted["total_points"][0] - team_data["total_points"][i]
        if points_differential > best_trade['points_differential']:
            best_trade['player_in'] = available_players_sorted["name"][0]
            best_trade['player_out'] = team_data["name"][i]
            best_trade['points_differential'] = points_differential
        
    return best_trade
        




    




