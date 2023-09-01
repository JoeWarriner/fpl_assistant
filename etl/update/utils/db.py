import pandas as pd
import sqlalchemy
from pathlib import Path
from sqlalchemy.orm import sessionmaker

import datetime

FILE_ROOT = Path(__file__).parents[1].joinpath('files')




def get_db_engine():
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    return engine


def query(query_str: str) -> pd.DataFrame:
    engine = get_db_engine()
    result = pd.DataFrame(engine.connect().execute(sqlalchemy.text(query_str)).all())
    return result

def get_all_teams() -> pd.DataFrame:
    teams = query('SELECT * FROM teams;')
    return teams

def get_team_from_fpl_id(fpl_id) -> int:
    team = f'SELECT id FROM teams WHERE fpl_id = {fpl_id};'
    return team['id'][0]

def get_current_season() -> int:
    current_season = query('SELECT id FROM seasons WHERE is_current = true;')
    return int(current_season['id'])

def get_current_season_start_year() -> int:
    current_season = query('SELECT start_year FROM seasons WHERE is_current = true;')
    return current_season['start_year'][0]

def update_new_season(current_year: int, this_season: str ):
    engine = get_db_engine()
    Session = sessionmaker(engine)

    with Session() as session:
        session.execute(sqlalchemy.text('UPDATE seasons SET is_current = FALSE'))
           
    new_season = pd.DataFrame(
            columns = ['is_current', 'start_year', 'season'],
            data = [[True, current_year, this_season]]
        )
    new_season.to_sql('seasons', con=engine, index=False, if_exists='append')


def get_team_from_fpl_seasonal_id(fpl_id: int) -> int:
    team_id = query(f'''
        SELECT teams.id 
        FROM 
            team_seasons 
            INNER JOIN teams ON team_seasons.team = teams.id
            INNER JOIN seasons ON seasons.id = team_seasons.season
        WHERE 
            team_seasons.fpl_id = {fpl_id} 
            AND
            seasons.start_year = '2023';
        '''
    )
    return team_id['id'][0]

def get_player_from_fpl_seasonal_id(fpl_id: int, season = None) -> int:
    if not season:
        season = get_current_season()
    player_details = query(f'''
        SELECT 
            players.id,
            players.second_name
        FROM 
            players INNER JOIN
            player_seasons ON player_seasons.player = players.id  
        WHERE 
            player_seasons.fpl_id = {fpl_id}
            AND player_seasons.season = {season};
        '''
    )
    try:
        return player_details['id'][0], player_details['second_name'][0]
    except KeyError as err:
        raise Exception(f'No record found for player with fpl id {fpl_id}, season {season}') from err
    


def get_player_fpl_id(id: int, season = None):
    if not season:
        season = get_current_season()
    player_details = query(f'''
        SELECT 
            player_seasons.fpl_id,
            players.second_name
        FROM 
            players INNER JOIN
            player_seasons ON player_seasons.player = players.id  
        WHERE 
            players.id = {id}
            AND player_seasons.season = {season};
        '''
    )
    try:
        return player_details['fpl_id'][0], player_details['second_name'][0]
    except KeyError as err:
        raise Exception(f'No record found for player {id} in season {season}') from err


def get_all_players_for_season(id: int, season = None):
    if not season:
        season = get_current_season()

    player_details = query(f'''
        SELECT 
            *
        FROM 
            players INNER JOIN
            player_seasons ON player_seasons.player = players.id      
            AND player_seasons.season = {season};
        '''
    )
    return player_details

def get_player_from_code(code: int):
    player_details = query(f'''
        SELECT 
            id
        FROM 
            players
        WHERE
            fpl_id = {code}
        '''
    )
    try:
        return player_details['id'][0]
    except KeyError as err:
        raise Exception(f'No record found for player with fpl_id {code}') from err
    

def get_position(fpl_id: str):
    position = query(f'SELECT id FROM positions WHERE fpl_id = {fpl_id};')
    return position['id'][0]


def get_fixture_from_fpl_id(fpl_id: str, season = None):
    if not season:
        season = get_current_season()

    fixtures = query(
    f'''
        SELECT id 
        FROM  fixtures 
        WHERE 
            fpl_id={fpl_id} AND 
            season = {season};
    '''
    )
    return fixtures['id'][0]

def get_latest_player_summaries():
    FILE_ROOT.joinpath()

if __name__ ==  "__main__":
    print(get_fixture_from_fpl_id(18))