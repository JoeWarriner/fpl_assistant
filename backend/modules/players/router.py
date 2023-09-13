from fastapi import APIRouter

from modules.shared_models import Player
from modules.players.db import get_players

router = APIRouter(
    prefix='/players'
)


@router.get('/', response_model=list[Player] )
async def players(offset: int = 0, pagesize: int = 10):
    players = get_players(offset, pagesize)
    return players

