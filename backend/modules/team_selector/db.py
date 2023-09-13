import pandas as pd 
from database.data_access_layer import dal
import database.tables as tbl

from modules.shared_models import Player
import modules.shared_queries as queries
from sqlalchemy import select

def get_player_data() -> pd.DataFrame:

    output = dal.session.execute(
        select(
            tbl.Player.id,
            tbl.Player.current_value,
            tbl.PlayerFixture.predicted_score,
            tbl.PlayerSeason.position_id,
            tbl.PlayerFixture.team_id
        )
        .select_from(
            tbl.PlayerFixture
        )
        .join(
            tbl.Player, 
            tbl.PlayerFixture.player_id == tbl.Player.id
        )
        .join(
            tbl.Fixture, 
            tbl.Fixture.id == tbl.PlayerFixture.fixture_id
        )
        .join(
            tbl.Gameweek, 
            tbl.Fixture.gameweek_id == tbl.Gameweek.id
        ).join(
            tbl.PlayerSeason, 
            (tbl.Player.id == tbl.PlayerSeason.player_id) &
            (tbl.PlayerSeason.season_id == tbl.Gameweek.season_id)
        )
        .where(
            tbl.Gameweek.is_next == True
        ).where(
            tbl.PlayerFixture.predicted_score != None
        )
        ).all()
    output = pd.DataFrame(output)
    
    return output


def get_players_from_list(player_ids: list[int]) -> list[Player]:
    players = []
    for player_id in player_ids:
        player_details = dal.session.execute(
            queries.PLAYERS.where(tbl.Player.id == player_id)
        ).one()
        
        player = Player(**player_details._mapping)
        players.append(player)
    return players
    
        
    