from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated, Union
from lp import get_clean_data, get_optimised_team, get_best_single_trade

app = FastAPI()

app.add_middleware(CORSMiddleware, allow_origins=['*'])
@app.get('/best-team')
async def best_team():
    '''
    Get an optimised team:
    http://localhost:8000/best-team
    '''
    
    data = get_clean_data()
    team = get_optimised_team(data)
    return team


@app.get('/best-trade/')
async def best_trade(current_players: Annotated[Union[list[str], None], Query()], spare_money: int) -> dict[str, Union[str, int]]:
    '''
    Find the best trade for a given team.

    e.g.:
    http://localhost:8000/best-trade/?current_players=James Maddison&current_players=Mohamed Salah&current_players=Andrew Robertson&current_players=Trent Alexander-Arnold&current_players=João Pedro Cavaco Cancelo&current_players=Aymeric Laporte&current_players=Cristiano Ronaldo dos Santos Aveiro&current_players=Hugo Lloris&current_players=Harry Kane&current_players=Heung-Min Son&current_players=Bukayo Saka&spare_money=5

    '''
    
    data = get_clean_data()
    trade = get_best_single_trade(current_players, data)
    return trade