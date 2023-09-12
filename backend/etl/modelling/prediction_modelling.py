from etl.jobs.base_job import Job

from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import select, Result, Tuple

class ModelPredictions(Job):
    window_size = 10

    def get_current_players(self) -> Result:
        result = dal.session.execute(
            select(
                tbl.Player
            ).join(
                tbl.PlayerSeason, tbl.Player.id == tbl.PlayerSeason.player_id
            ).join(
                tbl.Season, tbl.PlayerSeason.season_id == tbl.Season.id
            ).where(
                tbl.Season.is_current == True
            ))
        return result


    ## Select all players with the current player season.

    ## For each player:
        ## select their previous X performances. 
        ## take a mean of those performances
        ## get their fixtures
        ## insert prediction into those fixtures.


    ## Get the 


    ## For

    def run(self):
        pass