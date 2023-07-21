import pandas as pd
import psycopg2
from pathlib import Path
import os
import sqlalchemy
data_path = Path(os.getcwd(), 'etl', 'input', 'data')

seasons = [
    '2016-17',
    '2017-18',
    '2018-19',
    '2019-20',
    '2020-21',
    '2021-22',
    '2022-23',
]





def extract_players(): 
    player_data = pd.DataFrame(columns= ['first_name', 'second_name'])
    for season in seasons:
        season_player_data = pd.read_csv(data_path / season /'players_raw.csv', encoding = 'latin1')
        season_player_data = season_player_data[['first_name', 'second_name', 'code']]
        player_data = pd.concat([player_data, season_player_data])
        
    player_data = player_data.drop_duplicates(subset=['code']).rename(columns = {'code':'fpl_id'})
    player_data['current_team'] = pd.NA
    player_data['current_cost'] = pd.NA

    player_data.to_csv('etl/temp_data/players.csv', index=False)    
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

    player_data.to_sql('players', if_exists= 'append', con=engine, index=False)

def extract_teams():
    seasons_data = []
    for season in seasons[3:]:
        data = pd.read_csv(data_path / season /'teams.csv', encoding = 'latin1')
        seasons_data.append(data[['code', 'name', 'short_name']])
    
    team_data =  pd.concat(seasons_data).drop_duplicates(subset=['code']).rename(columns= {'code' : 'fpl_id', 'name': 'team_name'})
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

    team_data.to_sql('teams', if_exists='append', con = engine, index = False)

    ## Going to need to manually add some teams for earlier seasons.


    















    




if __name__ == "__main__":
    extract_players()
    extract_teams()