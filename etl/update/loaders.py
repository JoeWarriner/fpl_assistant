from typing import Any
from sqlalchemy.dialects.postgresql import insert
import etl.update.database as db
from etl.update.database import dal
from sqlalchemy.orm import Session, DeclarativeBase


COLS_TO_MATCH = {
    db.Gameweek: [db.Gameweek.season_id, db.Gameweek.gw_number],
    db.Player: [db.Player.fpl_id],
    db.Team: [db.Team.fpl_id],
    db.Season: [db.Season.start_year],
    db.Position: [db.Position.fpl_id],
    db.PlayerSeason: [db.PlayerSeason.fpl_id, db.PlayerSeason.player],
    db.PlayerFixture: [db.PlayerFixture.fixture, db.PlayerFixture.player],
    db.TeamSeason: [db.TeamSeason.team, db.TeamSeason.season]
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

        
        
        
