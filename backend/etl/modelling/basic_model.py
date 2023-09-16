from etl.jobs.base_job import Job
from abc import ABC
from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import select, Result, Tuple, Sequence, update
from decimal import Decimal
from datetime import datetime

class ModellingJob(Job, ABC):
    expects_input = False

    

class SimpleRollingMeanPrediction(ModellingJob):
    expects_input = False
    winodw_size: int
    today_date: datetime

    def __init__(self):
        self.window_size = 10
        self.today_date = datetime.now()

    def get_current_players(self) -> Result:
        result = dal.session.scalars(
            select(
                tbl.Player
            ).join(
                tbl.PlayerSeason, tbl.Player.id == tbl.PlayerSeason.player_id
            ).join(
                tbl.Season, tbl.PlayerSeason.season_id == tbl.Season.id
            ).where(
                tbl.Season.is_current == True
            )).all()
        return result
    

    def get_player_recent_peformances(self, player: tbl.Player) -> Sequence[int]:
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
        ).fetchmany(self.window_size)

        return result

    def calculate_mean(self, player_performances: list[int]) -> float: 
        sum = 0
        for performance in player_performances:
            sum += performance
        mean = sum / len(player_performances)

        return mean
    
    def get_player_future_fixtures(self, player: tbl.Player):
        
        output = dal.session.scalars(
            select(
                tbl.PlayerFixture.id
            ).select_from(
                tbl.PlayerFixture
            ).join(
                tbl.Fixture, tbl.PlayerFixture.fixture_id == tbl.Fixture.id
            ).where(
                tbl.PlayerFixture.player_id == player.id
            ).where(
                tbl.Fixture.kickoff_time > self.today_date
            )
        ).all()
        return output
    
    

            
    def run(self):
        players = self.get_current_players()
        prediction_data = {}
        for player in players:
            performances = self.get_player_recent_peformances(player)
            if len(performances) == self.window_size:
                prediction = self.calculate_mean(list(performances))
                future_fixture_ids = self.get_player_future_fixtures(player)
                for fixture in future_fixture_ids:
                    prediction_data[fixture] = prediction
        return prediction_data
        
                
            
            