from typing import Any
from sqlalchemy.dialects.postgresql import insert
import database.tables as tbl
from database.data_access_layer import dal
from sqlalchemy.orm import Session, DeclarativeBase


COLS_TO_MATCH = {
    tbl.Gameweek: [tbl.Gameweek.season_id, tbl.Gameweek.gw_number],
    tbl.Player: [tbl.Player.fpl_id],
    tbl.Team: [tbl.Team.fpl_id],
    tbl.Season: [tbl.Season.start_year],
    tbl.Position: [tbl.Position.fpl_id],
    tbl.PlayerSeason: [tbl.PlayerSeason.fpl_id, tbl.PlayerSeason.player],
    tbl.PlayerFixture: [tbl.PlayerFixture.fixture, tbl.PlayerFixture.player],
    tbl.TeamSeason: [tbl.TeamSeason.team, tbl.TeamSeason.season]
}
    
     
class DBLoader:
    def __init__(self, table: DeclarativeBase):
        self.table = table
    
    def load(self, data_dict): 
        insert_stmt = insert(
                self.table
            ).values(
                data_dict
            ).on_conflict_do_update(
                constraint= f'{self.table.__tablename__}__prevent_duplicate_import',
                set_=data_dict
            )
        dal.session.execute(insert_stmt)
        

        
        
        
