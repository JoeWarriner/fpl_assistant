import pandas as pd
import psycopg2
import json
import datetime
from pathlib import Path
import requests
import os
import sqlalchemy
data_path = Path(os.getcwd(), 'etl', 'input', 'data')

PRIOR_SEASONS_LABELS = [
    '2016-17',
    '2017-18',
    '2018-19',
    '2019-20',
    '2020-21',
    '2021-22',
    '2022-23',
]

def extract_seasons():
    seasons = pd.DataFrame(
        columns = ['is_current', 'start_year'],
        data = [
            [False, 2016],
            [False, 2017],
            [False, 2018],
            [False, 2019],
            [False, 2020],
            [False, 2021],
            [False, 2022],            
            [True, 2023]            
        ]
    )

    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    seasons.to_sql('seasons', if_exists= 'append', con=engine, index=False)

def extract_players(): 
    player_data = pd.DataFrame(columns= ['first_name', 'second_name'])
    for season in PRIOR_SEASONS_LABELS:
        season_player_data = pd.read_csv(data_path / season /'players_raw.csv', encoding = 'latin1')
        season_player_data = season_player_data[['first_name', 'second_name', 'code']]
        player_data = pd.concat([player_data, season_player_data])
        
    player_data = player_data.drop_duplicates(subset=['code']).rename(columns = {'code':'fpl_id'})

    player_data.to_csv('etl/temp_data/players.csv', index=False)    
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

    player_data.to_sql('players', if_exists= 'append', con=engine, index=False)

def extract_teams():
    seasons_data = []
    for season in PRIOR_SEASONS_LABELS:
        try:
            data = pd.read_csv(data_path / season /'teams.csv', encoding = 'latin1')
            seasons_data.append(data[['code', 'name', 'short_name']])
        except FileNotFoundError:
            print(f'No teams file for season: {season}')

    
    # The following teams only appeared in seasons where summarised team data isn't available.
    # Team codes derived manually from players in the squad that season.
    missing_teams = pd.DataFrame(
        columns = ['code',  'name', 'short_name'],
        data = [
            [56, 'Sunderland', 'SUN'],
            [25, 'Middlesbrough', 'MID'],
            [88, 'Hull', 'HUL'],
            [80, 'Swansea', 'SWA'],
            [110, 'Stoke', 'STO'],
            [97, 'Cardiff', 'CAR'],
            [38, 'Huddersfield', 'HUD'],
        ]
    )
    seasons_data.append(missing_teams)
    team_data =  pd.concat(seasons_data).drop_duplicates(subset=['code']).rename(columns= {'code' : 'fpl_id', 'name': 'team_name'})
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    team_data.to_sql('teams', if_exists='append', con = engine, index = False)


def extract_positions():
    api_data = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
    api_data_dict = api_data.json()
    positions = api_data_dict['element_types']
    positions_df = pd.DataFrame(positions)
    positions_df = positions_df[['id','singular_name', 'singular_name_short']].rename(columns= {'id': 'fpl_id', 'singular_name': 'pos_name', 'singular_name_short': 'short_name'})
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    positions_df.to_sql('positions', if_exists='append', con=engine, index=False)

def extract_gameweeks():
    seasons_list = []
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

    for season in PRIOR_SEASONS_LABELS:
        season_data = pd.read_csv(data_path / season / 'gws' / 'merged_gw.csv', encoding='latin1')
        season_gameweeks = season_data[['kickoff_time', 'GW']].groupby(['GW']).min()

        season_query = f'SELECT id FROM seasons where start_year = {season.split("-")[0]};'
        season_id = engine.connect().execute(sqlalchemy.text(season_query)).all()[0][0]
        season_gameweeks['season'] = season_id
        seasons_list.append(season_gameweeks)

    output = pd.concat(seasons_list).reset_index()
    output['is_current'] = False
    output['finished'] = True
    output['is_next'] = False
    output['is_previous'] = False
    output['kickoff_time'] = output['kickoff_time'].apply(lambda x: datetime.datetime.strptime(x, r'%Y-%m-%dT%H:%M:%SZ'))

    output.loc[output['kickoff_time'].idxmax(), 'is_previous'] = True
    output = output.rename(columns={'GW': 'gw_number', 'kickoff_time': 'deadline_time'})
    output.to_sql('gameweeks', if_exists='append', con=engine, index=False)






            


    

if __name__ == "__main__":
    extract_seasons()
    extract_players()
    extract_teams()
    extract_positions()
    extract_gameweeks()