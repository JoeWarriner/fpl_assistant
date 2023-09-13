from fastapi import APIRouter

from modules.shared_models import Player
import modules.team_selector.db as db

from modules.team_selector.optimiser import get_optimised_team

router = APIRouter(
    prefix='/team-selector'
)


@router.get('/', response_model=list[Player] )
async def team():

    all_players = db.get_player_data()
    team_ids = get_optimised_team(all_players)
    selected_players = db.get_players_from_list(team_ids)

    return selected_players
