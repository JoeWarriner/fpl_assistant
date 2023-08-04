import pandas as pd
import psycopg2
import json
import datetime
from typing import Union
from pathlib import Path
import requests
import os
import numpy as np
import sqlalchemy

data_path = Path(os.getcwd(), 'etl', 'input', 'data')

PRIOR_SEASONS_LABELS = [
    # '2016-17',
    # '2017-18',
    '2018-19',
    '2019-20',
    '2020-21',
    '2021-22',
    '2022-23',
]


def extract_seasons(engine: sqlalchemy.Engine):
    seasons = pd.DataFrame(
        columns = ['is_current', 'start_year', 'season'],
        data = [
            # [False, 2016],
            # [False, 2017],
            [False, 2018, '2018-19'],
            [False, 2019, '2019-20'],
            [False, 2020, '2020-21'],
            [False, 2021, '2021-22'],
            [False, 2022, '2022-23'],            
            [True, 2023, '2023-24']            
        ]
    )

    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    seasons.to_sql('seasons', if_exists= 'append', con=engine, index=False)

def get_data_for_all_seasons(engine: sqlalchemy.Engine, filename: Union[Path, str], columns = None, include_seasons_col = True) -> pd.DataFrame:
    season_query = f'SELECT id, season FROM seasons WHERE is_current = false  ;'
    seasons = engine.connect().execute(sqlalchemy.text(season_query)).all()

    season_data_list = []
    for season, season_str in seasons:
        try:
            season_data = pd.read_csv(data_path / season_str / filename, encoding = 'latin1')
            if columns:
                season_data = season_data[columns]
            if include_seasons_col:
                season_data['season'] = season
            season_data_list.append(season_data)
        except FileNotFoundError:
            print(f'No {filename} file found for season: {season_str}')
    
    all_season_data = pd.concat(season_data_list)
    return all_season_data


    

def extract_players(engine): 
    player_data = get_data_for_all_seasons(
                        engine, 
                        filename = Path('players_raw.csv'), 
                        columns=['first_name', 'second_name', 'code'], 
                        include_seasons_col=False
                    ).drop_duplicates(subset=['code']
                    ).rename(columns = {'code':'fpl_id'}
                    ).to_sql('players', if_exists= 'append', con=engine, index=False)

    

def extract_teams(engine):

    team_data = get_data_for_all_seasons(
                        engine, 
                        Path('teams.csv'), 
                        columns=['code', 'name', 'short_name'],
                        include_seasons_col=False
                    )   

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
    
    pd.concat([team_data, missing_teams]
        ).drop_duplicates(subset=['code']
        ).rename(columns= {'code' : 'fpl_id', 'name': 'team_name'}
        ).to_sql('teams', if_exists='append', con = engine, index = False)


def extract_team_seasons(engine: sqlalchemy.Engine):
    teams_query = f'SELECT id as team, fpl_id as fpl_perm_id FROM teams' 
    teams = pd.DataFrame(engine.connect().execute(sqlalchemy.text(teams_query)))

    get_data_for_all_seasons(engine, Path('players_raw.csv'), columns=['team', 'team_code']
        ).drop_duplicates(
        ).rename(columns={'team_code': 'fpl_perm_id', 'team': 'fpl_temp_id'}
        ).merge(teams, how='left', left_on=['fpl_perm_id'], right_on=['fpl_perm_id']
        ).drop(columns=['fpl_perm_id']
        ).rename(columns= {'fpl_temp_id':'fpl_id'}
        ).to_sql('team_seasons', if_exists='append', con=engine, index=False)
    
        

def extract_positions(engine: sqlalchemy.Engine):

    positions_api_data = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/"
                                      ).json(
                                      )['element_types']
    
    pd.DataFrame(positions_api_data
            )[['id','singular_name', 'singular_name_short']
            ].rename(columns= {'id': 'fpl_id', 'singular_name': 'pos_name', 'singular_name_short': 'short_name'}
            ).to_sql('positions', if_exists='append', con=engine, index=False)


def extract_player_seasons(engine):
    positions_query = f'SELECT id as position, fpl_id as position_fpl_id FROM positions;'
    positions = pd.DataFrame(engine.connect().execute(sqlalchemy.text(positions_query)))

    player_query = F'SELECT id as player, fpl_id as player_fpl_id FROM players;'
    players = pd.DataFrame(engine.connect().execute(sqlalchemy.text(player_query)))


    
    player_data = get_data_for_all_seasons(engine, Path('players_raw.csv'), columns =['code', 'id', 'element_type']
                    ).merge(positions, how='left', left_on=['element_type'], right_on=['position_fpl_id']
                    ).drop(columns=['element_type', 'position_fpl_id']
                    ).merge(players, how = 'left', left_on=['code'], right_on=['player_fpl_id']
                    ).drop(columns=['code', 'player_fpl_id']
                    ).rename(columns={'id':'fpl_id'}
                    ).to_sql('player_seasons', if_exists='append', con=engine, index=False)


def extract_gameweeks(engine: sqlalchemy.Engine):

    gameweek_data = get_data_for_all_seasons(engine, Path('gws','merged_gw.csv'), columns = ['kickoff_time', 'GW']
            ).groupby(['GW','season']
            ).min(
            ).reset_index(
            ).assign(
                is_current = False,
                is_next = False, 
                finished = True,
                deadline_time = lambda df: [datetime.datetime.strptime(value, r'%Y-%m-%dT%H:%M:%SZ') for value in df['kickoff_time']],
                is_previous = lambda df: [(value == df['deadline_time'].max()) for value in df['deadline_time']]
            ).drop(columns=['kickoff_time']
            ).rename(columns = {'GW':'gw_number'}
            ).to_sql('gameweeks', if_exists='append', con=engine, index=False)
    
    # gameweek_data.loc[gameweek_data['kickoff_time'].idxmax(), 'is_previous'] = True
        
        

def extract_fixtures(engine: sqlalchemy.Engine):

    team_season_query = f'SELECT fpl_id as team_season_id, team, season FROM team_seasons;'
    team_seasons = pd.DataFrame(engine.connect().execute(sqlalchemy.text(team_season_query)).all())

    gameweek_query = f'SELECT id as gameweek, season, gw_number FROM gameweeks;'
    gameweeks = pd.DataFrame(engine.connect().execute(sqlalchemy.text(gameweek_query)).all())

    fixtures_columns = ['event', 'finished', 'started', 'team_a', 'team_h', 'kickoff_time', 'id', 'team_a_score', 'team_h_score', 'team_a_difficulty', 'team_h_difficulty']
    fixtures_data = get_data_for_all_seasons(engine, Path('fixtures.csv'), columns = fixtures_columns
                        )
    

    fixtures_data = fixtures_data.merge(
                            team_seasons, 
                            how='left', 
                            left_on=['team_a', 'season'], 
                            right_on=['team_season_id', 'season']
                        ).drop(columns=['team_a','team_season_id']
                        ).rename(columns={'team': 'away_team'})
    
    fixtures_data = fixtures_data.merge(
                            team_seasons, 
                            how='left', 
                            left_on=['team_h', 'season'], 
                            right_on=['team_season_id', 'season']
                        ).drop(columns=['team_h','team_season_id']
                        ).rename(columns={'team': 'home_team'})
    
    fixtures_data = fixtures_data.merge(
                            gameweeks, 
                            how='left', 
                            left_on=['season', 'event'], 
                            right_on=['season', 'gw_number']
                        ).drop(columns=['gw_number', 'event']) 


    fixtures_data['kickoff_time'] = fixtures_data['kickoff_time'].apply(lambda x: datetime.datetime.strptime(x, r'%Y-%m-%dT%H:%M:%SZ'))

    fixtures_data.rename(
                    columns = {
                        'team_a_score': 'away_team_score', 
                        'team_h_score': 'home_team_score', 
                        'team_a_difficulty': 'away_team_difficulty',
                        'team_h_difficulty': 'home_team_difficulty',
                        'id': 'fpl_id'
                }).to_sql('fixtures', if_exists='append', con=engine, index=False)


def extract_player_fixtures(engine: sqlalchemy.Engine):
    ## IN PROGRESS
    
    fixtures_query = 'SELECT id as fixture, season, fpl_id as fpl_fixture_id, away_team, home_team from fixtures;'
    fixtures = pd.DataFrame(engine.connect().execute(sqlalchemy.text(fixtures_query)).all())

    players_query = 'SELECT player, season, fpl_id as fpl_player_season_id from player_seasons;'
    players = pd.DataFrame(engine.connect().execute(sqlalchemy.text(players_query)).all())

    player_fixtures_columns = [
        'element',
        'fixture',
        'value',
        'minutes',
        'penalties_missed',
        'penalties_saved',
        'red_cards',
        'yellow_cards',
        'selected',
        'total_points',
        'goals_scored',
        'goals_conceded',
        'clean_sheets',
        'bonus',
        'assists',
        'was_home'
    ]


    player_fixtures_data  = get_data_for_all_seasons(engine, Path('gws', 'merged_gw.csv'), columns=player_fixtures_columns
                            ).rename(columns= {
                                'fixture': 'fixture_id', 
                                'value': 'player_value',
                                'minutes': 'minutes_played',
                                'clean_sheets': 'clean_sheet'
                            })

    home_fixtures = player_fixtures_data[
                            player_fixtures_data['was_home'] == True
                        ].merge(
                            fixtures,
                            left_on = ['fixture_id', 'season'],
                            right_on = ['fpl_fixture_id', 'season'],
                        ).drop(columns = ['fixture_id','fpl_fixture_id']
                        ).rename(columns={'away_team': 'opposition', 'home_team': 'team'})
    
    away_fixtures = player_fixtures_data[
                            player_fixtures_data['was_home'] == False
                        ].merge(
                            fixtures,
                            left_on = ['fixture_id', 'season'],
                            right_on = ['fpl_fixture_id', 'season'],
                        ).drop(columns = ['fixture_id','fpl_fixture_id']
                        ).rename(columns={'away_team': 'team', 'home_team': 'opposition'})            
    
    pd.concat([home_fixtures, away_fixtures]
                            ).merge(players,
                                left_on= ['element', 'season'],
                                right_on= ['fpl_player_season_id', 'season']
                            ).astype({'clean_sheet': 'boolean'}
                            ).drop(columns=['element', 'fpl_player_season_id', 'season']
                            ).to_sql('player_fixtures', con=engine, index=False, if_exists='append')


def validate(engine):


    validation_query = '''
        SELECT 
            teams.short_name as team,
            positions.short_name as position
        FROM
            players 
            INNER JOIN player_fixtures on  players.id = player_fixtures.player
            INNER JOIN teams on player_fixtures.team = teams.id
            INNER JOIN player_seasons on players.id = player_seasons.player
            INNER JOIN positions on player_seasons.position = positions.id
            INNER JOIN fixtures on player_fixtures.fixture = fixtures.id
            INNER JOIN seasons on fixtures.season = seasons.id
        WHERE
            players.second_name = 'Salah'
            AND seasons.start_year > 2018
        ;

    '''
    validation_table = pd.DataFrame(engine.connect().execute(sqlalchemy.text(validation_query)).all())


    for column, expected_value in (('team', 'LIV') , ('position', 'MID')):
        try:
            assert validation_table[column].eq(expected_value).all()
        except AssertionError as e:
            print(validation_table.groupby(column).count())
        
    
    




                

def run():    
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

    extract_seasons(engine)
    extract_players(engine)
    extract_teams(engine)
    extract_team_seasons(engine)
    extract_positions(engine)
    extract_player_seasons(engine)
    extract_gameweeks(engine)
    extract_fixtures(engine)
    extract_player_fixtures(engine)
    validate(engine)

