import requests
import sqlalchemy
from typing import Callable
from utils.file_handlers import ProjectFiles
import pandas as pd
import numpy as np
import utils.db as db
from typing import Any, Union
from datetime import datetime








# latest_data = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/').json()

# START OF SEASON UPDATE:

## Add new season:

def add_new_season(engine):
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_season_start_year = db.get_current_season_start_year()
    if current_year != current_season_start_year and current_month > 7:
        this_season = f'{current_year}-{str(current_year + 1)[-2:]}' # e.g. 2022-23
        db.update_new_season(current_year, this_season)
    
## Add new teams:
def get_new_teams():
    current_teams_db = db.get_all_teams()
    current_teams_api = ProjectFiles.teams_json
    new_teams = []
    for team in current_teams_api['teams']:
        if team['code'] not in list(current_teams_db['fpl_id']):
            new_teams.append(team)
    return new_teams


TEAM_FIELD_MAPPING = {
    'fpl_id': 'code',
    'team_name': 'name',
    'short_name': 'short_name'
}


def add_new_teams(new_teams):
    new_team_records = {field: [] for field in TEAM_FIELD_MAPPING.keys()}
    
    for team in new_teams:
        for db_field, api_field in TEAM_FIELD_MAPPING.items():
            new_team_records[db_field].append(team[api_field])
                                                
    pd.DataFrame(new_team_records
            ).to_sql('teams', con=engine, index=False, if_exists='append')



def update_team_seasons(engine):
    current_teams_api = ProjectFiles.teams_json
    current_season = db.get_current_season()
    new_team_seasons = {
        'fpl_id': [],
        'team': [],
        'season': []
    }
    for team in current_teams_api:
        new_team_seasons['fpl_id'].append(team['id'])
        new_team_seasons['team'].append(db.get_team_from_fpl_id(team['code']))
        new_team_seasons['season'].append(current_season)
    
    pd.DataFrame(
                new_team_seasons
        ).to_sql('team_seasons', con=engine, index=False, if_exists='append')


## Update players:

# def update_players(engine):
#     current_players = utils.query('SELECT * FROM players')
#     for player in latest_data['elements']:
#         if player['code'] not in list(current_players['fpl_id']):
#             pd.DataFrame(
#                 columns = ['fpl_id', 'first_name', 'second_name'],
#                 data = [[player['code'], player['first_name'], player['second_name']]]
#             ).to_sql('players', con=engine, index=False, if_exists='append')
    
# ## Update player seasons:
# def update_player_seasons(engine):
#     player_now_costs = {}
#     current_season = utils.query('SELECT id FROM seasons WHERE is_current = true;')
#     for player in latest_data['elements']:
#         player_id = utils.query(f'SELECT id FROM players WHERE fpl_id = {player["code"]};')
#         position_id = utils.query(f'SELECT id FROM positions WHERE fpl_id = {player["element_type"]};')
#         pd.DataFrame(
#                 columns = ['fpl_id', 'player', 'position', 'season'],
#                 data = [[player['id'], int(player_id['id']), int(position_id['id']), int(current_season['id']) ]]
#             ).to_sql('player_seasons', con=engine, index=False, if_exists='append')
#         player_now_costs[int(player_id['id'])] = player['now_cost']
#     return player_now_costs


def check_fields_all_added(
        table_dict: dict[str, list[Any]]
        ):
    first_field_length = len(list(table_dict.values())[0])
    for field in table_dict.values():
        assert len(field) == first_field_length


def api_converter(
        input_field: Union[str, int, bool],  
        function : Callable = None, 
        constant : Union[str, int, bool ] = None
        ) -> Union[str, int, bool ] :
    
    if function: return function(input_field)
    if constant: return constant
    else: return input_field
    

def add_api_item_to_dict(
        api_dict: dict[str,Any], 
        table_dict: dict[str, list[Any]], 
        row_mapping: tuple[tuple[str, str, Callable, Any]]
        ) -> dict[str, list[Any]]:
    
    for api_field, table_field, func, constant in row_mapping:
        table_dict[table_field].append(api_converter(api_dict.get(api_field), func, constant))
    check_fields_all_added(table_dict)


def create_db_update_dict(
        api_records: list[dict[str,Any]], 
        api_to_db_mapping: tuple[tuple[str, str, Callable, Any]]
    ) -> dict[str, list[Any]]:
    db_update_dict = {db_field: [] for _, db_field, _, _ in api_to_db_mapping}
    for item in api_records:
        add_api_item_to_dict(item, db_update_dict, api_to_db_mapping)
    return db_update_dict

def update_db_from_api_data(
        api_dict: list[dict[str, Any]], 
        api_to_db_mapping: tuple[tuple[str, str, Callable, Any]], 
        db_table_name: str):
    db_update_dict = create_db_update_dict(api_dict, api_to_db_mapping)
    db_update_df = pd.DataFrame(db_update_dict)
    print(db_update_df)
    # db_update_df.to_sql(db_table_name, con=engine, index=False, if_exists='append')


def filter_for_new_records(
        api_dict: list[dict[str,Any]], 
        db_table: pd.DataFrame,
        comparator_fields: dict[str, str]
    ):
    new_records = []
    for record in api_dict:
        for api_field, db_field in comparator_fields.items():
            if record[api_field] not in db_table[db_field]:
                new_records.append(record)
    return new_records


def update_new_records(
        api_dict: list[dict[str, Any]], 
        db_table: pd.DataFrame, 
        db_table_name: str, 
        comparator_fields: dict[str, str], 
        api_to_db_mapping: tuple[tuple[str, str, Callable, Any]]
        ):
    api_dict_new_records = filter_for_new_records(api_dict, db_table, comparator_fields)              
    update_db_from_api_data(api_dict_new_records, api_to_db_mapping, db_table_name)




def parse_time(time):
    return datetime.strptime(time, r'%Y-%m-%dT%H:%M:%SZ')



## Update gameweeks:





def update_gameweeks():
    api_db_mapping = (
        ('deadline_time', 'deadline_time', parse_time, None),
        ('finished', 'finished', None, None),
        ('is_previous', 'is_previous', None, None),
        ('is_next', 'is_next', None, None),
        ('is_current', 'is_current', None, None),
        ('id', 'gw_number', None, None),
        ('season', None, None, db.get_current_season()),
    )
    gameweeks = ProjectFiles.gameweeks_json
    update_db_from_api_data(gameweeks, api_db_mapping, 'gameweeks')
    

    # for gameweek in gameweeks:

    #     deadline_time = datetime.strptime(gameweek['deadline_time'], r'%Y-%m-%dT%H:%M:%SZ')
    #     pd.DataFrame(
    #         columns = ['deadline_time' , 'finished' , 'is_previous' , 'is_next' , 'is_current' , 'gw_number' , 'season'],
    #         data = [[deadline_time, gameweek['finished'], gameweek['is_previous'], gameweek[''], gameweek['is_current'], gameweek['id'], int(current_season['id'])]]
    #     ).to_sql('gameweeks', con=engine, index=False, if_exists='append')


## Update fixtures:



def update_fixtures(engine):
    fixtures_data = requests.get('https://fantasy.premierleague.com/api/fixtures/').json()
    for fixture in fixtures_data:
        
        if fixture['event'] is not None: 
            gameweek_id = int(utils.query(f'SELECT gameweeks.id FROM gameweeks INNER JOIN seasons ON gameweeks.season = seasons.id WHERE gw_number={fixture["event"]} AND seasons.start_year=2023')['id'])
        else:
            gameweek_id = None
        
        kickoff_time = datetime.strptime(fixture['kickoff_time'], r'%Y-%m-%dT%H:%M:%SZ') if fixture['kickoff_time'] is not None else None
        pd.DataFrame(
            columns = ['gameweek' , 'away_team' , 'home_team' , 'away_team_difficulty' , 'home_team_difficulty', 'season', 'away_team_score', 'home_team_score', 'fpl_id', 'kickoff_time', 'finished', 'started'],
            data = [[gameweek_id, utils.get_team_from_fpl_seasonal_id(fixture['team_a']), utils.get_team_from_fpl_seasonal_id(fixture['team_h']), fixture['team_a_difficulty'], fixture['team_h_difficulty'], 6,  fixture['team_a_score'], fixture['team_h_score'], fixture['id'], kickoff_time, fixture['started'], fixture['finished']]]
        ).to_sql('fixtures', con=engine, index=False, if_exists='append')


## Update player fixtures
def update_player_fixtures(player_now_costs, engine):
    players_this_season = utils.query('SELECT player, fpl_id FROM player_seasons JOIN seasons ON player_seasons.season = seasons.id WHERE seasons.start_year = 2023')
    for i, row in players_this_season.iterrows():
        print(f'{i} / {len(players_this_season.index)})')
        player_fixtures_data = requests.get(f'https://fantasy.premierleague.com/api/element-summary/{row["fpl_id"]}/').json()
        for fixture in player_fixtures_data['fixtures']:
            fpl_team_id = fixture['team_h'] if fixture['is_home'] else fixture['team_a']
            fpl_opposition_id = fixture['team_a'] if fixture['is_home'] else fixture['team_h']
            team = utils.get_team_from_fpl_seasonal_id(fpl_team_id)
            opposition = utils.get_team_from_fpl_seasonal_id(fpl_opposition_id)
            fixture_id = utils.query(f'SELECT fixtures.id FROM fixtures INNER JOIN seasons ON fixtures.season = seasons.id WHERE fpl_id={fixture["id"]} AND seasons.start_year = 2023')
            pd.DataFrame(
            columns = ['fixture' , 'player' , 'team' , 'opposition' , 'player_value', 'minutes_played', 'penalties_missed', 'penalties_saved', 'red_cards', 'yellow_cards', 'selected', 'total_points', 'goals_scored', 'clean_sheet', 'bonus', 'assists', 'was_home'],
            data = [[int(fixture_id['id']), row['player'],  team, opposition, player_now_costs[row['player']], None , None, None,  None, None, None, None, None, None, None, None, fixture['is_home'] ]]
            ).to_sql('player_fixtures', con=engine, index=False, if_exists='append')


if __name__ == '__main__':
    # engine = utils.get_db_engine()
    # add_new_season(engine)
    # add_new_teams(engine)
    # update_team_seasons(engine)
    # update_players(engine)
    # player_now_costs = update_player_seasons(engine)
    update_gameweeks()
    # update_fixtures(engine)
    # update_player_fixtures(player_now_costs, engine)

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







    

    
    

    
