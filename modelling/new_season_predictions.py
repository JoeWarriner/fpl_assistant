import pandas as pd
import numpy as np
import sqlalchemy
from sklearn.linear_model import LinearRegression

engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")


players = 'SELECT * FROM players'


series_query = '''
    SELECT  
        gameweeks.id as gameweek_id,
        gameweeks.gw_number as gameweek,
        fixtures.id as fixture_id,
        fixtures.away_team_difficulty as away_team_difficulty,
        fixtures.home_team_difficulty as home_team_difficulty,
        player_fixtures.player as player,
        player_fixtures.minutes_played as minutes_played,
        player_fixtures.total_points as points,
        player_fixtures.was_home as was_home,
        seasons.start_year as season
    FROM 
        player_fixtures 
        LEFT JOIN fixtures on  player_fixtures.fixture = fixtures.id
        LEFT JOIN gameweeks on gameweeks.id = fixtures.gameweek
        LEFT JOIN seasons on fixtures.season = seasons.id
'''

training_series_query = series_query + '''WHERE seasons.start_year < 2022 '''
test_series_query = series_query + '''WHERE seasons.start_year = 2022 and gameweeks.gw_number <= 10'''


def query(query_str: str):  
    data = pd.DataFrame(engine.connect().execute(sqlalchemy.text(query_str)).all())
    return data


def compute_mae(test_series, actual_series):
    output = np.mean(np.abs(test_series - actual_series))
    return output

    
def get_expected_points_based_on_team(training_data:pd.DataFrame, test_data: pd.DataFrame, method = 'absolute'):
    if method == 'absolute':
        training_data['opposition_difficulty'] = [row['home_team_difficulty'] if row['was_home'] else row['away_team_difficulty'] for i, row in training_data.iterrows()]
        test_data['opposition_difficulty'] = [row['home_team_difficulty'] if row['was_home'] else row['away_team_difficulty'] for i, row in test_data.iterrows()]
    if method == 'ratio':
        training_data['opposition_difficulty'] = [row['home_team_difficulty'] / row['away_team_difficulty'] if row['was_home'] else row['away_team_difficulty'] / row['home_team_difficulty'] for i, row in training_data.iterrows()]
        test_data['opposition_difficulty'] = [row['home_team_difficulty'] / row['away_team_difficulty'] if row['was_home'] else row['away_team_difficulty'] / row['home_team_difficulty'] for i, row in test_data.iterrows()]

    players_to_evaluate = list(training_data.merge(test_data, how='inner', on = 'player')['player'].drop_duplicates())
    player_predictions_list = []
    for player in players_to_evaluate:
        player_training_data = training_data[training_data['player'] == player]
        # Have the output be away_team_difficulty if was_home = True, else home team difficulty
    
        model = LinearRegression()
        model.fit(np.array(player_training_data['opposition_difficulty']).reshape(-1,1), player_training_data['points'])

        player_test_data = test_data.loc[test_data['player'] == player,:].copy()       
        test_data_array = np.array(player_test_data['opposition_difficulty']).reshape(-1,1) 
        prediction = model.predict(test_data_array)

        player_test_data['prediction_on_opp_difficulty'] = prediction
        player_predictions_list.append(player_test_data)
    
    result = pd.concat(player_predictions_list)
    return result



def expected_points_based_on_play_probability(training_data:pd.DataFrame, test_data: pd.DataFrame):
    last_season_series = training_data[training_data['season'] == 2021]
    last_season_series['played'] = last_season_series['minutes_played'] > 60
    play_probability = last_season_series.groupby('player')['played'].mean().reset_index().rename(columns = {'played': 'play_prob'})
    
    last_ten_games_series = last_season_series[last_season_series['gameweek'] > 28]
    av_points_when_plays = last_ten_games_series[last_ten_games_series['played']].groupby('player')['points'].mean().reset_index()


    print(play_probability)
    print(av_points_when_plays)

    output = play_probability.merge(av_points_when_plays)
    output['expected_points_inc_play_chance'] = output['play_prob'] * output['points']
    output = output.drop(columns = ['play_prob', 'points'])
    test_data = test_data.merge(output, how = 'left')
    return test_data




    





if __name__ == '__main__':

    training_series = query(training_series_query)

    career_pl_average = training_series.groupby('player')['points'].mean().reset_index().rename(columns = {'points':'career_average_points'})

    last_season_series = training_series[training_series['season'] == 2021]
    last_season_average = last_season_series.groupby('player')['points'].mean().reset_index().rename(columns = {'points':'last_season_average_points'})

    last_season_median = last_season_series.groupby('player')['points'].median().reset_index().rename(columns = {'points':'last_season_median_points'})

    last_ten_gameweeks_series = last_season_series[last_season_series['gameweek'] > 28]
    last_ten_gameweeks_average = last_ten_gameweeks_series[training_series['season'] == 2021].groupby('player')['points'].mean().reset_index().rename(columns = {'points':'last_10_games_average_points'})
    

    test_series = query(test_series_query).rename(columns = {'points': 'actual_points'})
    test_series = get_expected_points_based_on_team(training_series, test_series).rename(columns = {'prediction_on_opp_difficulty': 'prediction_on_opp_abs_difficulty'})
    test_series = get_expected_points_based_on_team(training_series, test_series, method = 'ratio').rename(columns = {'prediction_on_opp_difficulty': 'prediction_on_difficulty_ratio'})
    
    test_series = expected_points_based_on_play_probability(training_series, test_series)


    result = test_series.merge(career_pl_average, how = 'left', on='player').merge(last_season_average, how = 'left', on='player').merge(last_ten_gameweeks_average, how='left', on='player').merge(last_season_median, how='left', on='player')
    ## Comparing models by how well they predict game by game scores in the first 10 games.

    print(f'MAE for career PL average {compute_mae(result["actual_points"], result["career_average_points"] )}')
    print(f'MAE for last season average {compute_mae(result["actual_points"], result["last_season_average_points"] )}')
    print(f'MAE for last season median {compute_mae(result["actual_points"], result["last_season_median_points"] )}')
    print(f'MAE for last ten games average {compute_mae(result["actual_points"], result["last_10_games_average_points"] )}')
    print(f'MAE for opponent difficulty prediction {compute_mae(result["actual_points"], result["prediction_on_opp_abs_difficulty"] )}')
    print(f'MAE for opponent difficulty ratio prediction {compute_mae(result["actual_points"], result["prediction_on_difficulty_ratio"] )}')
    print(f'MAE for expected points inc play chance {compute_mae(result["actual_points"], result["expected_points_inc_play_chance"] )}')

    output_cols = [
        'career_average_points',
        'last_season_average_points',
        'last_season_median_points',
        'last_10_games_average_points',
        'prediction_on_opp_abs_difficulty',
        'prediction_on_difficulty_ratio',
        'expected_points_inc_play_chance'
    ]

    other_cols = ['player', 'actual_points']
    
    all_cols = output_cols + other_cols

    result = result[all_cols].groupby('player').sum().reset_index()
    ## Comparing models by how well they predict the combined first 10 games scores
    print('COMBINED FIRST 10')
    for output in output_cols:
        print(f'MAE for {output} : {compute_mae(result["actual_points"], result[output] )}')

    player_values = query('''
                        SELECT 
                          player_fixtures.player,
                          player_fixtures.player_value
                        FROM 
                          player_fixtures 
                          JOIN fixtures on player_fixtures.fixture = fixtures.id 
                          JOIN gameweeks on fixtures.gameweek = gameweeks.id
                          JOIN seasons on fixtures.season = seasons.id
                        WHERE
                            seasons.start_year = 2021 AND
                            gameweeks.gw_number = 38
                        ''')
    
    result_with_values = result.merge(player_values, how = 'inner')
    result_with_values.loc[:,'actual_points_by_value'] = result_with_values['actual_points'] / result_with_values['player_value']
    print(result_with_values)
    result_with_values_top_100 = result_with_values.sort_values('actual_points_by_value', ascending=False).head(100).reset_index(drop=True).reset_index().rename(columns = {'index': 'actual_ranking'})
    result_with_values_top_100 = result_with_values_top_100[['actual_ranking', 'player', 'actual_points']]
    print(result_with_values_top_100)

    print('')
    print('BY TOTAL RANKING')
    ## Comparing models by how well they predict the top 100 players (by value) ranking
    for col in output_cols: 
        temp_df = result_with_values[[col, 'player', 'player_value']].copy()
        temp_df['by_value'] = temp_df[col] / temp_df['player_value']
        temp_df = temp_df.sort_values('by_value', ascending=False).reset_index(drop=True).reset_index().rename(columns = {'index': f'{col}_predicted_ranking'}).drop(columns = ['player_value', 'by_value'])
        result_with_values_top_100 = result_with_values_top_100.merge(temp_df, how='left', on=['player'])
        
        print(f'MAE for {col} : {compute_mae(result_with_values_top_100["actual_ranking"], result_with_values_top_100[ f"{col}_predicted_ranking"] )}')
    
    print('') 
    print('TOP 100 BY POINTS') 
    ## Comparing models by how well they predict the top 100 players (by value) points

    for col in output_cols:
        print(f'Accuracy for top 100 performers {col} : {compute_mae(result_with_values_top_100["actual_points"], result_with_values_top_100[col] )}')

    print(result_with_values_top_100)


    ## RANDOM FOREST IDEAS

    ## Recent form, last 5 fixtures (played): points, goals, assists, clean sheets. Expected all of the above.
    ## Recent form, last 10 fixtures (played): 
    ## PL quality
    ## Quality at current team.
    ## Total points in previous season (games played)
    
    ## Typical play against team like this

    ## Career play percentage
    ## Play percentage last 10 fixtures.
    ## Play percentage at current team
    ## Play percentage vs opposition team type

    

    ## Opposition team ranking
    ## Own team ranking
    ## Team differential

    ## FPL position
    ## Actual position      



    

    ## Team average goals per game. 
    

    ## Opposition quality.
    ## Opposition points given up to similar players.

         


    ## Form at current team.
    ## Length of time at current team.
    ## Players in same position at current team








        

