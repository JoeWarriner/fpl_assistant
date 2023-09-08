import database.tables as tbl
from database.data_access_layer import dal
from modules.players.models import Player
from sqlalchemy import select



def get_players(offset: int, page_size: int) -> list[Player]:
    players = dal.session.execute(
            select(
                tbl.Player.id,
                tbl.Player.first_name, 
                tbl.Player.second_name, 
                tbl.Position.short_name
            ).select_from(
                tbl.Player
            ).join(
                tbl.PlayerSeason, tbl.Player.id == tbl.PlayerSeason.player_id
            ).join(
                tbl.Position, tbl.PlayerSeason.position_id == tbl.Position.id
            ).join(
                tbl.Season, tbl.PlayerSeason.season_id == tbl.Season.id
            ).where(
                tbl.Season.is_current == True
            ).order_by(
                tbl.Player.id
            ).offset(offset).limit(page_size)
        )

    players = [
        Player(id= id, first_name = fname, second_name = sname, position = pos) 
        for id, fname, sname, pos 
        in players
    ]

    return players