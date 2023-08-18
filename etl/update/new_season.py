import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker

import pandas as pd
import numpy as np
from datetime import datetime

engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")

def query(query_str: str) -> pd.DataFrame:
    result = pd.DataFrame(engine.connect().execute(sqlalchemy.text(query_str)).all())
    return result



latest_data = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/').json()

# START OF SEASON UPDATE:

## Add new season:

def add_new_season():
    current_year = datetime.now().year
    seasons = query('SELECT * FROM seasons;')
    if current_year not in list(seasons['start_year']):
        Session = sessionmaker(engine)
        with Session() as session:
            session.execute(sqlalchemy.text('UPDATE seasons SET is_current = FALSE'))
            session.commit()
                        
        this_season = f'{current_year}-{str(current_year + 1)[-2:]}' # e.g. 2022-23
        new_season = pd.DataFrame(
            columns = ['is_current', 'start_year', 'season'],
            data = [[True, current_year, this_season]]
        )
        new_season.to_sql('seasons', con=engine, index=False, if_exists='append')
    
   
## Add new teams:
def add_new_teams():
    current_teams = query('SELECT * FROM teams;')
    for team in latest_data['teams']:
        if team['code'] not in list(current_teams['fpl_id']):
            pd.DataFrame(
                columns= ['fpl_id', 'team_name', 'short_name'],
                data = [[team['code'],team['name'], team['short_name']]]
            ).to_sql('teams', con=engine, index=False, if_exists='append')



def update_team_seasons():
    current_teams = query('SELECT * FROM teams;')
    current_season = query('SELECT id FROM seasons WHERE is_current = true;')

    for team in latest_data['teams']:
        team_id = query(f'SELECT id FROM teams WHERE fpl_id = {team["code"]};')
        pd.DataFrame(
                columns = ['fpl_id', 'team', 'season'],
                data = [[team['id'], int(team_id['id']), int(current_season['id'])]]
        ).to_sql('team_seasons', con=engine, index=False, if_exists='append')


## Update players:

def update_players():
    current_players = query('SELECT * FROM players')
    for player in latest_data['elements']:
        if player['code'] not in list(current_players['fpl_id']):
            pd.DataFrame(
                columns = ['fpl_id', 'first_name', 'second_name'],
                data = [[player['code'], player['first_name'], player['second_name']]]
            ).to_sql('players', con=engine, index=False, if_exists='append')
    
## Update player seasons:
def update_player_seasons():
    player_now_costs = {}
    current_players = query('SELECT * FROM players')
    current_season = query('SELECT id FROM seasons WHERE is_current = true;')
    for player in latest_data['elements']:
        player_id = query(f'SELECT id FROM players WHERE fpl_id = {player["code"]};')
        position_id = query(f'SELECT id FROM positions WHERE fpl_id = {player["element_type"]};')
        pd.DataFrame(
                columns = ['fpl_id', 'player', 'position', 'season'],
                data = [[player['id'], int(player_id['id']), int(position_id['id']), int(current_season['id']) ]]
            ).to_sql('player_seasons', con=engine, index=False, if_exists='append')
        player_now_costs[int(player_id['id'])] = player['now_cost']
    return player_now_costs



## Update gameweeks:
def update_gameweeks():
    for gameweek in latest_data['events']:
        deadline_time = datetime.strptime(gameweek['deadline_time'], r'%Y-%m-%dT%H:%M:%SZ')
        pd.DataFrame(
            columns = ['deadline_time' , 'finished' , 'is_previous' , 'is_next' , 'is_current' , 'gw_number' , 'season'],
            data = [[deadline_time, gameweek['finished'], gameweek['is_previous'], gameweek['is_next'], gameweek['is_current'], gameweek['id'], int(current_season['id'])]]
        ).to_sql('gameweeks', con=engine, index=False, if_exists='append')


## Update fixtures:

def get_team(fpl_team: int) -> int:
    team_id = query(f'''
        SELECT teams.id 
        FROM 
            team_seasons 
            INNER JOIN teams ON team_seasons.team = teams.id
            INNER JOIN seasons ON seasons.id = team_seasons.season
        WHERE 
            team_seasons.fpl_id = {fpl_team} 
            AND
            seasons.start_year = '2023';
        '''
    )
    return int(team_id['id'])

def update_fixtures():
    fixtures_data = requests.get('https://fantasy.premierleague.com/api/fixtures/').json()
    for fixture in fixtures_data:
        
        if fixture['event'] is not None: 
            gameweek_id = int(query(f'SELECT gameweeks.id FROM gameweeks INNER JOIN seasons ON gameweeks.season = seasons.id WHERE gw_number={fixture["event"]} AND seasons.start_year=2023')['id'])
        else:
            gameweek_id = None
        
        kickoff_time = datetime.strptime(fixture['kickoff_time'], r'%Y-%m-%dT%H:%M:%SZ') if fixture['kickoff_time'] is not None else None
        pd.DataFrame(
            columns = ['gameweek' , 'away_team' , 'home_team' , 'away_team_difficulty' , 'home_team_difficulty', 'season', 'away_team_score', 'home_team_score', 'fpl_id', 'kickoff_time', 'finished', 'started'],
            data = [[gameweek_id, get_team(fixture['team_a']), get_team(fixture['team_h']), fixture['team_a_difficulty'], fixture['team_h_difficulty'], 6,  fixture['team_a_score'], fixture['team_h_score'], fixture['id'], kickoff_time, fixture['started'], fixture['finished']]]
        ).to_sql('fixtures', con=engine, index=False, if_exists='append')


## Update player fixtures
def update_player_fixtures(player_now_costs):
    players_this_season = query('SELECT player, fpl_id FROM player_seasons JOIN seasons ON player_seasons.season = seasons.id WHERE seasons.start_year = 2023')
    for i, row in players_this_season.iterrows():
        print(f'{i} / {len(players_this_season.index)})')
        player_fixtures_data = requests.get(f'https://fantasy.premierleague.com/api/element-summary/{row["fpl_id"]}/').json()
        for fixture in player_fixtures_data['fixtures']:
            fpl_team_id = fixture['team_h'] if fixture['is_home'] else fixture['team_a']
            fpl_opposition_id = fixture['team_a'] if fixture['is_home'] else fixture['team_h']
            team = get_team(fpl_team_id)
            opposition = get_team(fpl_opposition_id)
            fixture_id = query(f'SELECT fixtures.id FROM fixtures INNER JOIN seasons ON fixtures.season = seasons.id WHERE fpl_id={fixture["id"]} AND seasons.start_year = 2023')
            pd.DataFrame(
            columns = ['fixture' , 'player' , 'team' , 'opposition' , 'player_value', 'minutes_played', 'penalties_missed', 'penalties_saved', 'red_cards', 'yellow_cards', 'selected', 'total_points', 'goals_scored', 'clean_sheet', 'bonus', 'assists', 'was_home'],
            data = [[int(fixture_id['id']), row['player'],  team, opposition, player_now_costs[row['player']], None , None, None,  None, None, None, None, None, None, None, None, fixture['is_home'] ]]
            ).to_sql('player_fixtures', con=engine, index=False, if_exists='append')


# Get and save api snapshot.


# What do we actually want to update?
# Players:
    # Need to add any additional players.
    # Chance of playing next round = null -> sign that 
    # status: 'u', ''

# player_seasons:
    # Need to add any additional players.
    # Is it a problem that 
    # Capture chance of playing
# player_fixtures
# fixtures







    

    
    

    
