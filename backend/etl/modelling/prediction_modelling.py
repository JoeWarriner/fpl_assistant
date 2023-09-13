from etl.jobs.base_job import Job

from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import select, Result, Tuple, Sequence, update
from decimal import Decimal
from datetime import datetime

class ModelPredictions(Job):
    expects_input = False

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
    
    
    def update_future_fixture_predictions(self, player_fixture_ids: list[int], prediction: float):
        for player_fixture_id in player_fixture_ids:
            dal.session.execute(
                update(tbl.PlayerFixture)  
                .where(tbl.PlayerFixture.id == player_fixture_id)
                .values(predicted_score = prediction)
            )
        
            


    def run(self):
        players = self.get_current_players()
        for player in players:
            performances = self.get_player_recent_peformances(player)
            if len(performances) == self.window_size:
                prediction = self.calculate_mean(list(performances))
                future_fixture_ids = self.get_player_future_fixtures(player)
                self.update_future_fixture_predictions(
                    player_fixture_ids=list(future_fixture_ids), 
                    prediction = prediction
                )
            
            