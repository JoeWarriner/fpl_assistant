from __future__ import annotations
from sqlalchemy.dialects.postgresql import insert
from typing import Any, Optional
from etl.jobs.transformers.base_transformer import Transformer
from abc import ABC, abstractmethod
from sqlalchemy import select
from sqlalchemy.orm import Session, DeclarativeBase
from database.data_access_layer import dal
import database.tables as tbl
import etl.jobs.extractors.api.api_models as api_models

class APITransformer(Transformer):
    def __init__(self, adapter: type[Adapter]):
        self.adapter = adapter
    
    def run(self, input_list):
        adapter = self.adapter()
        output = [adapter.convert(input) for input in input_list]
        return output




class Adapter:
    input: object
    table_ref: type[DeclarativeBase]
    table: DeclarativeBase

    def __init__(self):
        self.table = self.table_ref()
 
    def transform(self):
        ...
    
    @property
    def db_columns(self):
        return [col.key for col in self.table.__table__.columns if col.key != 'id']

    def convert(self, input) -> dict[str, Any]:
        self.input = input
        self.transform()
        output = {}
        for col in self.db_columns:
            if hasattr(self, col):
                output[col] = self.__dict__.get(col)
            elif hasattr(self.input, col):
                output[col] = self.input.__dict__.get(col)
            else:
                raise KeyError(
                    f'Unable to convert {self.input.__class__} to {self.table.__tablename__}.',
                    f'No data provided for field: {col}. '
                )
        return output        
            
    
class PlayerAdapter(Adapter):
    input: api_models.Player
    table_ref = tbl.Player

    def transform(self):
        self.fpl_id = self.input.code


class PositionAdapter(Adapter):
    input: api_models.Position
    table_ref = tbl.Position

    def transform(self):
        self.fpl_id = self.input.id
        self.pos_name = self.input.singular_name
        self.short_name = self.input.singular_name_short


class PlayerSeason(Adapter):
    input: api_models.Player
    table_ref = tbl.PlayerSeason

    def transform(self):
        self.fpl_id = self.input.id
        self.player_id = dal.session.scalar(
            select(tbl.Player.id).where(tbl.Player.fpl_id == self.input.code)
        )
        self.season_id = get_season()
        self.position_id = dal.session.scalar(select(tbl.Position.id).where(tbl.Position.fpl_id == self.input.element_type))
        

class TeamAdapter(Adapter):
    input: api_models.Team
    table_ref = tbl.Team

    def transform(self):
        self.fpl_id = self.input.code
        self.team_name = self.input.name
        

class TeamSeasonAdapter(Adapter):
    input: api_models.Team
    table_ref = tbl.TeamSeason

    def transform(self):
        self.fpl_id = self.input.id
        self.season_id = get_season()
        self.team_id = dal.session.scalar(select(tbl.Team.id).where(tbl.Team.fpl_id == self.input.code))


class GameWeekAdapter(Adapter):
    input: api_models.GameWeek
    table_ref = tbl.Gameweek

    def transform(self):
        self.gw_number = self.input.id
        self.season_id = get_season()



class FixtureAdapter(Adapter):
    input: api_models.Fixture
    table_ref = tbl.Fixture

    def transform(self):
        self.gameweek_id = dal.session.scalar(select(tbl.Gameweek.id).where(tbl.Gameweek.gw_number  == self.input.event))
        self.away_team_id = get_team(self.input.team_a)
        self.home_team_id = get_team(self.input.team_h)
        self.away_team_difficulty = self.input.team_a_difficulty
        self.home_team_difficulty = self.input.team_h_difficulty
        self.season_id = get_season()
        self.away_team_score = self.input.team_a_score
        self.home_team_score = self.input.team_h_score
        self.fpl_id = self.input.id
        self.fpl_code = self.input.code
        



class PlayerFixtureAdapter(Adapter):
    input: api_models.PlayerFixture
    table_ref = tbl.PlayerFixture


    def transform(self):
        self.fixture_id = get_fixture_id(self.input.id)
        self.player_id = get_player_id(self.input.element)
        self.opposition_id = self.get_opposition()
        self.team_id = self.get_team_played_for()
    

    def get_team_played_for(self):
        if self.input.is_home:
            return get_team(self.input.team_h)
        else:
            return get_team(self.input.team_a)
    
    def get_opposition(self):
        if self.input.is_home:
                return get_team(self.input.team_a)
        else:
            return get_team(self.input.team_h)
        

class PlayerPerformanceAdapter(Adapter):
    input: api_models.PlayerPerformance
    table_ref = tbl.PlayerPerformance
    
    def transform(self):
            self.fixture_id  = get_fixture_id(self.input.fixture)
            self.player_id = get_player_id(self.input.element)
            self.team_id = self.get_team_played_for(self.fixture_id)
            self.opposition_id = get_team(self.input.opponent_team)
            self.minutes_played = self.input.minutes
            self.clean_sheet = self.input.clean_sheets
            self.player_value = self.input.value
                

    def get_team_played_for(self, fixture_id):
        if self.input.was_home:
            field = tbl.Fixture.home_team_id
        else: 
            field = tbl.Fixture.away_team_id

        return dal.session.scalar(select(field).where(tbl.Fixture.id == fixture_id))


## DB FUNCTIONS:
 
def get_season(season_start_year = None):

    if not season_start_year:
        return dal.session.scalar(select(tbl.Season.id).where(tbl.Season.is_current == True))
    else:
        return dal.session.scalar(select(tbl.Season.id).where(tbl.Season.start_year == season_start_year))

def get_fixture_id(fixture_fpl_id: int):
        return dal.session.scalar(
                select(tbl.Fixture.id
                     ).join(tbl.Fixture.season
                              ).where(tbl.Fixture.fpl_id == fixture_fpl_id).where(tbl.Season.is_current == True)
                        )

def get_player_id(player_fpl_season_id: int):
        return dal.session.scalar(
            select(tbl.PlayerSeason.player_id
                   ).join(tbl.PlayerSeason.season
                          ).where(tbl.PlayerSeason.fpl_id == player_fpl_season_id and tbl.Season.is_current == True)
                )    

def get_team(team_fpl_season_id: int):
    return dal.session.scalar(
            select(tbl.Team.id
                ).join(tbl.Team.team_seasons).join(tbl.TeamSeason.season
                    ).where(tbl.TeamSeason.fpl_id == team_fpl_season_id and tbl.Season.is_current == True)
        )



        




