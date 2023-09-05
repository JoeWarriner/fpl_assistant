from __future__ import annotations
from sqlalchemy.dialects.postgresql import insert
from typing import Any, Optional
from abc import ABC, abstractmethod
from sqlalchemy import select
from sqlalchemy.orm import Session, DeclarativeBase
from etl.update.database import dal
import etl.update.database as db
import etl.update.api as api


def get_season(season_start_year = None):

    if not season_start_year:
        return dal.session.scalar(select(db.Season.id).where(db.Season.is_current == True))
    else:
        return dal.session.scalar(select(db.Season.id).where(db.Season.start_year == season_start_year))

def get_fixture_id(fixture_fpl_id: int):
        return dal.session.scalar(
                select(db.Fixture.id
                     ).join(db.Fixture.season
                              ).where(db.Fixture.id == fixture_fpl_id and db.Season.is_current == True)
                        )

def get_player_id(player_fpl_season_id: int):
        return dal.session.scalar(
            select(db.PlayerSeason.player_id
                   ).join(db.PlayerSeason.season
                          ).where(db.PlayerSeason.fpl_id == player_fpl_season_id and db.Season.is_current == True)
                )    

def get_team(team_fpl_season_id: int):
    return dal.session.scalar(
            select(db.Team.id
                ).join(db.Team.team_seasons).join(db.TeamSeason.season
                    ).where(db.TeamSeason.fpl_id == team_fpl_season_id and db.Season.is_current == True)
        )
    
class APITranformer:
    def __init__(self, adapter: type[Adapter]):
        self.adapter = adapter
    
    def convert(self, input_list):
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
    input: api.Player
    table_ref = db.Player

    def transform(self):
        self.fpl_id = self.input.code


class PositionAdapter(Adapter):
    input: api.Position
    table_ref = db.Position

    def transform(self):
        self.fpl_id = self.input.id
        self.pos_name = self.input.singular_name
        self.short_name = self.input.singular_name_short


class PlayerSeason(Adapter):
    input: api.Player
    table_ref = db.PlayerSeason

    def transform(self):
        self.fpl_id = self.input.id
        self.player_id = dal.session.scalar(
            select(db.Player.id).where(db.Player.fpl_id == self.input.code)
        )
        self.season_id = get_season()
        self.position_id = dal.session.scalar(select(db.Position.id).where(db.Position.fpl_id == self.input.element_type))
        

class TeamAdapter(Adapter):
    input: api.Team
    table_ref = db.Team

    def transform(self):
        self.fpl_id = self.input.code
        self.team_name = self.input.name
        

class TeamSeasonAdapter(Adapter):
    input: api.Team
    table_ref = db.TeamSeason

    def transform(self):
        self.fpl_id = self.input.id
        self.season_id = get_season()
        self.team_id = dal.session.scalar(select(db.Team.id).where(db.Team.fpl_id == self.input.code))


class GameWeekAdapter(Adapter):
    input: api.GameWeek
    table_ref = db.Gameweek

    def transform(self):
        self.gw_number = self.input.id
        self.season_id = get_season()



class FixtureAdapter(Adapter):
    input: api.Fixture
    table_ref = db.Fixture

    def transform(self):
        self.gameweek_id = dal.session.scalar(select(db.Gameweek.id).where(db.Gameweek.gw_number  == self.input.event))
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
    input: api.PlayerFixture
    table_ref = db.PlayerPerformance

    def __init__(self, input, player_fpl_season_id: int = None) -> None:
        super().__init__(input)
        self.player_fpl_season_id = player_fpl_season_id

    def transform(self):
        self.fixture_id = get_fixture_id(self.input.id)
        self.player_id = get_player_id(self.player_fpl_season_id)
        self.opposition_id = self.get_opposition()
        self.team_id = self.get_team_played_for()
    

    def get_team_played_for(self):
        if self.input.is_home:
            return get_team(self.input.team_h)
        else:
            return get_team(self.input.team_a)
    
    def get_opposition(self):
        if not self.input.is_home:
                return get_team(self.input.team_a)
        else:
            return get_team(self.input.team_h)
        

class PlayerPerformanceAdapter(Adapter):
    input: api.PlayerPerformance
    table_ref = db.PlayerPerformance

    def __init__(self, input, player_fpl_season_id: int = None) -> None:
        super().__init__(input)
        self.player_fpl_season_id = player_fpl_season_id
    
    def transform(self):
            self.fixture_id  = get_fixture_id(self.input.fixture)
            self.player_id = get_player_id(self.player_fpl_season_id)
            self.team = self.get_team_played_for(self.fixture_id)
            self.opposition_id = get_team(self.input.opponent_team)
            self.minutes_played = self.input.minutes
            self.clean_sheet = self.input.clean_sheets
                

    def get_team_played_for(self, fixture_id):
        if self.input.was_home:
            field = db.Fixture.home_team_id
        else: 
            field = db.Fixture.away_team_id

        return dal.session.scalar(select(field).where(db.Fixture.id == fixture_id))

            

            

    

    
        




