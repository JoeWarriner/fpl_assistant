import database.tables as tbl
from database.data_access_layer import dal

from modules.shared_models import Player
import modules.shared_queries as queries



def get_players(offset: int, page_size: int) -> list[Player]:
    players = dal.session.execute(
            queries.PLAYERS.offset(offset).limit(page_size)
        ).all()

    players = [
        Player(**player._mapping) 
        for player
        in players
    ]

    return players