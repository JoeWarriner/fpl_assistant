from typing import Union
import pulp
import pandas as pd 


def get_best_single_trade(team: list[str], player_data: pd.DataFrame) -> dict[str,  Union[str, int]]:

    # Get data for team.
    player_data = pd.read_pickle('data/player_data.pkl')
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


    




