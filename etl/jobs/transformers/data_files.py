from typing import Callable
from database.data_access_layer import dal
from datetime import datetime
import pandas as pd
import sqlalchemy

class DataFileTransformer:
    dataframe: pd.DataFrame

    def __init__(self):
        self.seasons_table = None

    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def get_seasons_table(self):
        if  self.seasons_table is  None:
            season_query = 'SELECT id as season_id, season FROM seasons' 
            self.seasons_table = pd.DataFrame(dal.session.execute(sqlalchemy.text(season_query)))

    def convert_season_ids(self, data: pd.DataFrame):
        self.get_seasons_table()
        data = data.merge(
            self.seasons_table, how = 'left', on=['season']
            ).drop(columns = ['season'])
        return data

    def convert(self, data: pd.DataFrame):
        data = self.convert_season_ids(data)
        dataframe = self.do_transformations(data)            
        return dataframe.to_dict(orient='records')


class PlayerTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame):
        data =  data[
            ['first_name', 'second_name', 'code']
                    ].drop_duplicates(subset=['code']
                    ).rename(columns = {'code':'fpl_id'})
        return data

class TeamTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
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
        data = pd.concat([data[['code', 'name', 'short_name']], missing_teams]
            ).drop_duplicates(subset=['code']
            ).rename(columns= {'code' : 'fpl_id', 'name': 'team_name'})
        return data
    

class TeamSeasonTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        teams_query = f'SELECT id as team, fpl_id as fpl_perm_id FROM teams' 
        teams = pd.DataFrame(dal.session.execute(sqlalchemy.text(teams_query)))

        data = data[['team', 'team_code', 'season_id']].drop_duplicates(
            ).rename(columns={'team_code': 'fpl_perm_id', 'team': 'fpl_temp_id'}
            )
        

        data = data.merge(teams, how='left', left_on=['fpl_perm_id'], right_on=['fpl_perm_id']
            ).drop(columns=['fpl_perm_id']
            ).rename(columns= {'fpl_temp_id':'fpl_id', 'team': 'team_id'})
        
        
        return data


class PlayerSeasonTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        positions_query = f'SELECT id as position, fpl_id as position_fpl_id FROM positions;'
        positions = pd.DataFrame(dal.session.execute(sqlalchemy.text(positions_query)))

        player_query = F'SELECT id as player, fpl_id as player_fpl_id FROM players;'
        players = pd.DataFrame(dal.session.execute(sqlalchemy.text(player_query)))

        player_data = data[['code', 'id', 'element_type', 'season_id']].merge(
                        positions, how='left', left_on=['element_type'], right_on=['position_fpl_id']
                        ).drop(columns=['element_type', 'position_fpl_id']
                        ).merge(players, how = 'left', left_on=['code'], right_on=['player_fpl_id']
                        ).drop(columns=['code', 'player_fpl_id']
                        ).rename(columns={'id':'fpl_id', 'player': 'player_id', 'position': 'position_id'})
        
        return player_data

class GameWeekTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        gameweek_data = data[['kickoff_time', 'GW', 'season_id']]
        gameweek_data = gameweek_data.groupby(['GW','season_id']
            ).min(
            ).reset_index(
            ).assign(
                is_current = False,
                is_next = False, 
                finished = True,
                deadline_time = lambda df: [datetime.strptime(value, r'%Y-%m-%dT%H:%M:%SZ') for value in df['kickoff_time']],
                is_previous = lambda df: [(value == df['deadline_time'].max()) for value in df['deadline_time']]
            ).drop(columns=['kickoff_time']
            ).rename(columns = {'GW':'gw_number'})
        return gameweek_data
    

class FixturesTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:

        fixtures_columns = ['event', 'finished', 'started', 'team_a', 'team_h', 'kickoff_time', 'id', 'team_a_score', 'team_h_score', 'team_a_difficulty', 'team_h_difficulty', 'season_id', 'code']
        fixtures_data = data[fixtures_columns]


        team_season_query = f'SELECT fpl_id as team_season_id, team_id, season_id as season FROM team_seasons;'
        team_seasons = pd.DataFrame(dal.session.execute(sqlalchemy.text(team_season_query)).all())
        print(team_seasons)

        gameweek_query = f'SELECT id as gameweek_id, season_id as season, gw_number FROM gameweeks;'
        gameweeks = pd.DataFrame(dal.session.execute(sqlalchemy.text(gameweek_query)).all())
    

        fixtures_data = fixtures_data.merge(
                            team_seasons, 
                            how='left', 
                            left_on=['team_a', 'season_id'], 
                            right_on=['team_season_id', 'season']
                        ).drop(columns=['team_a','team_season_id', 'season']
                        ).rename(columns={'team_id': 'away_team_id'})
    
        fixtures_data = fixtures_data.merge(
                                team_seasons, 
                                how='left', 
                                left_on=['team_h', 'season_id'], 
                                right_on=['team_season_id', 'season']
                            ).drop(columns=['team_h','team_season_id', 'season'], 
                            ).rename(columns={'team_id': 'home_team_id'})
        
        fixtures_data = fixtures_data.merge(
                                gameweeks, 
                                how='left', 
                                left_on=['season_id', 'event'], 
                                right_on=['season', 'gw_number']
                            ).drop(columns=['gw_number', 'event', 'season']) 
        
        fixtures_data['kickoff_time'] = fixtures_data['kickoff_time'].apply(lambda x: datetime.strptime(x, r'%Y-%m-%dT%H:%M:%SZ'))

        fixtures_data = fixtures_data.rename(
                    columns = {
                        'team_a_score': 'away_team_score', 
                        'team_h_score': 'home_team_score', 
                        'team_a_difficulty': 'away_team_difficulty',
                        'team_h_difficulty': 'home_team_difficulty',
                        'id': 'fpl_id',
                        'code': 'fpl_code'
                })
        
        return fixtures_data
    

class PlayerPerformanceTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:

        fixtures_query = 'SELECT id as fixture, season_id, fpl_id as fpl_fixture_id, away_team_id, home_team_id from fixtures;'
        fixtures = pd.DataFrame(dal.session.execute(sqlalchemy.text(fixtures_query)).all())

        players_query = 'SELECT player_id, season_id, fpl_id as fpl_player_season_id from player_seasons;'
        players = pd.DataFrame(dal.session.execute(sqlalchemy.text(players_query)).all())

        player_performances_columns = [
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
            'was_home',
            'season_id'
        ]

        player_performances_data  = data[player_performances_columns].rename(columns= {
                                    'fixture': 'fixture_id', 
                                    'value': 'player_value',
                                    'minutes': 'minutes_played',
                                    'clean_sheets': 'clean_sheet'
                                })
        
        print('BREAK 1 - PLAYER PERFORMANCES')
        print(player_performances_data[['element', 'was_home', 'fixture_id']])
        print('BREAK 1 - FIXTURES')
        print(fixtures[['fpl_fixture_id', 'away_team_id', 'home_team_id']])
        
        home_fixtures = player_performances_data[
                                player_performances_data['was_home'] == True
                            ].merge(
                                fixtures,
                                left_on = ['fixture_id', 'season_id'],
                                right_on = ['fpl_fixture_id', 'season_id'],
                            ).drop(columns = ['fixture_id','fpl_fixture_id']
                            ).rename(columns={'away_team_id': 'opposition_id', 'home_team_id': 'team_id'})
        
        print('BREAK 2 - HOME FIXTURES')
        print(home_fixtures[['fixture', 'opposition_id', 'team_id', 'element', 'was_home']])
        

        away_fixtures = player_performances_data[
                                player_performances_data['was_home'] == False
                            ].merge(
                                fixtures,
                                left_on = ['fixture_id', 'season_id'],
                                right_on = ['fpl_fixture_id', 'season_id'],
                            ).drop(columns = ['fixture_id','fpl_fixture_id']
                            ).rename(columns={
                                'away_team_id': 'team_id', 
                                'home_team_id': 'opposition_id'
                            })            
        
        print('BREAK 3 - AWAY FIXTURES')
        print(away_fixtures[['fixture', 'opposition_id', 'team_id', 'element', 'was_home']])

        player_performances_data = pd.concat([home_fixtures, away_fixtures]
                                ).merge(players,
                                    left_on= ['element', 'season_id'],
                                    right_on= ['fpl_player_season_id', 'season_id']
                                ).astype({'clean_sheet': 'boolean'}
                                ).drop(columns=['element', 'fpl_player_season_id', 'season_id']).rename(columns={'fixture':'fixture_id'})
        print('BREAK 4 - COMBINED DATA')
        print(player_performances_data[['fixture_id', 'opposition_id', 'team_id', 'player_id']])

        print('BREAK 5 - Player seasons')
        print(players)
        return player_performances_data