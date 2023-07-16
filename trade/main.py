from fastapi import FastAPI, Query
from best_trade import get_best_single_trade
from typing import Annotated, Union

app = FastAPI()

@app.get('/best-trade/')
async def best_trade(current_players: Annotated[Union[list[str], None], Query()], spare_money: int) -> dict[str, Union[str, int]]:
    '''
    Find the best trade for a given team.

    e.g.:
    http://localhost:8000/best-trade/?current_players=James Maddison&current_players=Mohamed Salah&current_players=Andrew Robertson&current_players=Trent Alexander-Arnold&current_players=João Pedro Cavaco Cancelo&current_players=Aymeric Laporte&current_players=Cristiano Ronaldo dos Santos Aveiro&current_players=Hugo Lloris&current_players=Harry Kane&current_players=Heung-Min Son&current_players=Bukayo Saka&spare_money=5

    '''

    trade = get_best_single_trade(current_players)
    return trade