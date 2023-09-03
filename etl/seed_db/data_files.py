from typing import Callable
from etl.update.database import dal
from datetime import datetime
import pandas as pd
import sqlalchemy

class DataFileTransformer:
    dataframe: pd.DataFrame


    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def convert_season_ids(self, data: pd.DataFrame):
        season_query = f'SELECT id, season FROM seasons' 
        seasons = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(season_query)))
        print(seasons)
        data = data.merge(
            seasons, how = 'left', on=['season']
            ).drop(columns = ['season']
            ).rename(columns ={'id': 'season'})
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
        data = pd.concat([data.drop(columns='season'), missing_teams]
            ).drop_duplicates(subset=['code']
            ).rename(columns= {'code' : 'fpl_id', 'name': 'team_name'})
        return data
    

class TeamSeasonTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        teams_query = f'SELECT id as team, fpl_id as fpl_perm_id FROM teams' 
        teams = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(teams_query)))
        data = data[['team', 'team_code', 'season']].drop_duplicates(
            ).rename(columns={'team_code': 'fpl_perm_id', 'team': 'fpl_temp_id'}
            ).merge(teams, how='left', left_on=['fpl_perm_id'], right_on=['fpl_perm_id']
            ).drop(columns=['fpl_perm_id']
            ).rename(columns= {'fpl_temp_id':'fpl_id'})


class PlayerSeasonTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        positions_query = f'SELECT id as position, fpl_id as position_fpl_id FROM positions;'
        positions = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(positions_query)))

        player_query = F'SELECT id as player, fpl_id as player_fpl_id FROM players;'
        players = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(player_query)))

        player_data = data[['code', 'id', 'element_type', 'season']].merge(
                        positions, how='left', left_on=['element_type'], right_on=['position_fpl_id']
                        ).drop(columns=['element_type', 'position_fpl_id']
                        ).merge(players, how = 'left', left_on=['code'], right_on=['player_fpl_id']
                        ).drop(columns=['code', 'player_fpl_id']
                        ).rename(columns={'id':'fpl_id'})
        
        return player_data

class GameWeekTransformer(DataFileTransformer):
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        gameweek_data = data[['kickoff_time', 'GW', 'season']].groupby(['GW','season']
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

        fixtures_columns = ['event', 'finished', 'started', 'team_a', 'team_h', 'kickoff_time', 'id', 'team_a_score', 'team_h_score', 'team_a_difficulty', 'team_h_difficulty']
        fixtures_data = data[fixtures_columns]


        team_season_query = f'SELECT fpl_id as team_season_id, team, season FROM team_seasons;'
        team_seasons = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(team_season_query)).all())

        gameweek_query = f'SELECT id as gameweek, season, gw_number FROM gameweeks;'
        gameweeks = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(gameweek_query)).all())

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
        
        fixtures_data['kickoff_time'] = fixtures_data['kickoff_time'].apply(lambda x: datetime.strptime(x, r'%Y-%m-%dT%H:%M:%SZ'))

        fixtures_data = fixtures_data.rename(
                    columns = {
                        'team_a_score': 'away_team_score', 
                        'team_h_score': 'home_team_score', 
                        'team_a_difficulty': 'away_team_difficulty',
                        'team_h_difficulty': 'home_team_difficulty',
                        'id': 'fpl_id'
                })
        
        return fixtures_data
    

class PlayerFixturesTransformer:
    def do_transformations(self, data: pd.DataFrame) -> pd.DataFrame:

        fixtures_query = 'SELECT id as fixture, season, fpl_id as fpl_fixture_id, away_team, home_team from fixtures;'
        fixtures = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(fixtures_query)).all())

        players_query = 'SELECT player, season, fpl_id as fpl_player_season_id from player_seasons;'
        players = pd.DataFrame(dal.engine.connect().execute(sqlalchemy.text(players_query)).all())

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

        player_fixtures_data  = data[player_fixtures_columns].rename(columns= {
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
        
        player_fixtures_data = pd.concat([home_fixtures, away_fixtures]
                                ).merge(players,
                                    left_on= ['element', 'season'],
                                    right_on= ['fpl_player_season_id', 'season']
                                ).astype({'clean_sheet': 'boolean'}
                                ).drop(columns=['element', 'fpl_player_season_id', 'season']
                                ).to_sql('player_fixtures', con=dal.engine, index=False, if_exists='append') 

        return player_fixtures_data