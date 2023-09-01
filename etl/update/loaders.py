from typing import Any
from sqlalchemy.dialects.postgresql import insert
from etl.update.database import Gameweek, Player, dal, Team
from sqlalchemy.orm import Session, DeclarativeBase


COLS_TO_MATCH = {
    Gameweek: [Gameweek.season_id, Gameweek.gw_number],
    Player: [Player.fpl_id],
    Team: [Team.fpl_id]
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
                index_elements= COLS_TO_MATCH[self.table],
                set_=data_dict
            )
        dal.session.execute(insert_stmt)

        
        
        
