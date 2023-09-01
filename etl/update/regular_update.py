from utils.file_handlers import ProjectFiles
from typing import Any
import pandas as pd
import utils.db as db


def get_new_players():
    '''
    Get data from the API for all players who don't have a record for this season yet.
    '''
    current_players_api = pd.DataFrame(ProjectFiles.player_overview_json)
    current_players_db = db.get_all_players_for_season()
    
    new_players = []
    for player in current_players_api:
        if player['code'] not in current_players_db['fpl_id']:
            new_players.append(player)

    return new_players


def add_new_player_records(new_players):
    new_player_records = []
    for player in new_players:
            new_player_records.append(
                [
                    player['code'], 
                    player['first_name'], 
                    player['second_name']
                ]
            )

    new_player_df = pd.DataFrame(
         columns = ['fpl_id', 'first_name', 'second_name'],
         data = new_player_records
    )
    new_player_df.to_sql('players', con = db.get_db_engine(), if_exists='append', index=False)


def add_new_player_season_records(new_players):
    season = db.get_current_season()

    new_player_seasons = []
    for player in new_players:
            new_player_seasons.append(
                [
                    player['id'],
                    db.get_player_from_code(code = player['code']),
                    season,
                    db.get_position(fpl_id= player['element_type']),
                ]
            )

    new_player_season_df = pd.DataFrame(
        columns = ['fpl_id', 'player', 'season', 'position'],
        data = new_player_seasons
    )

    new_player_season_df.to_sql('player_seasons', con = db.get_db_engine(), if_exists='append', index=False)





PRE_GAME_FIXTURE_FIELDS = [
    'fixture',
    'player',
    'team',
    'opposition',
    'player_value',
    'was_home'
]

POST_GAME_FIXTURE_FIELDS = [
        'minutes_played',
        'penalties_missed',
        'penalties_saved',
        'red_cards',
        'yellow_cards',
        'selected',
        'total_points',
        'goals_scored',
        'goals_conceded',
        'clean_sheet',
        'bonus',
        'assists'
    ]

POST_GAME_NAME_MAPPING = {
    'minutes_played': 'minutes'
}


def add_common_pre_game_fields(new_player_fixtures: dict[str, list], fixture: dict[str, Any], player: dict[str, Any], player_id: int):
    fpl_team_id = fixture['team_h'] if fixture['is_home'] else fixture['team_a']
    fpl_opposition_id = fixture['team_a'] if fixture['is_home'] else fixture['team_h']
    
    new_player_fixtures['fixture'].append(db.get_fixture_from_fpl_id(fixture['id']))
    new_player_fixtures['player'].append(player_id)
    new_player_fixtures['team'].append(db.get_team_from_fpl_seasonal_id(fpl_team_id))
    new_player_fixtures['opposition'].append(db.get_team_from_fpl_seasonal_id(fpl_opposition_id))
    return new_player_fixtures

def check_fields_all_added(new_player_fixtures: dict[str, list]):
    current_length = len(new_player_fixtures['fixture'])
    for field in new_player_fixtures.values():
        assert len(field) == current_length


def add_new_player_fixtures(new_players):
    all_fields = PRE_GAME_FIXTURE_FIELDS + POST_GAME_FIXTURE_FIELDS
    new_player_fixtures = { field: [] for field in all_fields}

    
    for player in new_players:
        player_detail = ProjectFiles.get_player_detail_json(player['id'], player['second_name'])
        player_id = db.get_player_from_code(player['code'])
        
        for fixture in player_detail['fixtures']:
            new_player_fixtures = add_common_pre_game_fields(new_player_fixtures, fixture, player, player_id)
            new_player_fixtures['was_home'].append(fixture['is_home'])
            new_player_fixtures['player_value'].append(player['now_cost'])
            for field in POST_GAME_FIXTURE_FIELDS:
                 new_player_fixtures[field] = None
            check_fields_all_added(new_player_fixtures)
        
        for fixture in player_detail['history']:
            new_player_fixtures = add_common_pre_game_fields(new_player_fixtures, fixture, player, player_id)
            new_player_fixtures['was_home'].append(fixture['was_home'])
            new_player_fixtures['player_value'].append(fixture['value'])
            for field in POST_GAME_FIXTURE_FIELDS:
                api_field_name = POST_GAME_NAME_MAPPING.get(field, field)
                new_player_fixtures[field].append(fixture[api_field_name])

    new_player_fixtures_df = pd.DataFrame(new_player_fixtures)
    new_player_fixtures_df.to_sql('player_fixtures', con = db.get_db_engine(), if_exists='append', index=False)
                    
                     




            
            



            


        

    

    


    

        

    


