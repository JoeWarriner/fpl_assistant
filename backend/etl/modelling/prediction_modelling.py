from etl.jobs.base_job import Job

from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import select, Result, Tuple, Sequence, update
from decimal import Decimal
from datetime import datetime

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
    

    def get_player_recent_peformances(self, player: tbl.Player, number = 10) -> Sequence[int]:
        result = dal.session.scalars(
            select(
                tbl.PlayerPerformance.total_points
            ).join(
                tbl.Fixture, tbl.PlayerPerformance.fixture_id == tbl.Fixture.id
            ).where(
                tbl.PlayerPerformance.player_id == player.id
            ).where(
                tbl.PlayerPerformance.minutes_played > 60
            ).order_by(
                tbl.Fixture.kickoff_time.desc()
            )
        ).fetchmany(number)

        return result

    def calculate_mean(self, player_performances: list[int]) -> float: 
        sum = 0
        for performance in player_performances:
            sum += performance
        mean = sum / len(player_performances)

        return mean
    
    def get_player_future_fixtures(self, player: tbl.Player, today_date = datetime.now()):
        output = dal.session.scalars(
            select(
                tbl.PlayerFixture.id
            ).select_from(
                tbl.PlayerFixture
            ).join(
                tbl.Fixture, tbl.PlayerFixture.fixture_id == tbl.Fixture.id
            ).where(
                tbl.PlayerFixture.player_id == player.id
            )
        ).all()

        return output
    
    
    # def update_future_fixture_predictions(self, player: tbl.Player, prediction: float):
    #     update(tbl.PlayerFixture).join(

    #     ).where(
    #         tbl.PlayerFixture.player_id
    #     )

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
        # players = self.get_current_players().all()
        # for player in players:
        #     performances = self.get_player_recent_peformances(player)
            
            