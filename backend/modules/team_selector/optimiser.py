import pulp
import pandas as pd 

def get_optimised_team(player_data: pd.DataFrame) -> list[str]:

    ## Extract relevant variables
    players = player_data['id']
    teams = player_data['team_id']

    distinct_teams = teams.unique()

    predicted_score = player_data['predicted_score']
    cost = player_data['current_value']

    is_gk = player_data['position_id'] == 1
    is_mid = player_data['position_id'] == 3
    is_def = player_data['position_id'] == 2
    is_fwd = player_data['position_id'] == 4

    model = pulp.LpProblem("Integer_Programming_Problem", pulp.LpMaximize)

    ## Objective
    x = pulp.LpVariable.dicts('x', players, cat='Binary')
    model += pulp.lpSum([predicted_score[i] * x[player] for i, player in enumerate(players)])

    ## Constraints
    model += pulp.lpSum([is_gk[i] * x[player] for i, player in enumerate(players)]) == 2
    model += pulp.lpSum([is_mid[i] * x[player] for i, player in enumerate(players)]) == 5
    model += pulp.lpSum([is_def[i] * x[player] for i, player in enumerate(players)]) == 5
    model += pulp.lpSum([is_fwd[i] * x[player] for i, player in enumerate(players)]) == 3
    model += pulp.lpSum([cost[i] * x[player] for i, player in enumerate(players)]) <= 1000

    for team in distinct_teams:
        model += pulp.lpSum([(teams[i] == team) * x[player] for i, player in enumerate(players)]) <= 3

    ## Solve and return:
    model.solve()
    selected_players  = [player for player in players if x[player].value() == 1]
    return(selected_players)

