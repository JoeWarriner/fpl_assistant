from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database.data_access_layer import dal
import database.tables as tbl
from typing import Optional
from pydantic import BaseModel
from sqlalchemy import select
import json

app = FastAPI()

dal.connect()
dal.session = dal.Session()

app.add_middleware(CORSMiddleware, allow_origins=['*'])

@app.get('/best-team')
async def best_team():
    '''
    Get an optimised team:
    http://localhost:8000/best-team
    '''
    
    with open('data/current_best_team.json') as file:
        team = json.loads(file.read())
        
    return team


class Player(BaseModel):
    first_name: str
    second_name: str
    position: str


@app.get('/players', response_model=list[Player] )
async def get_players(skip: int = 0, limit: int = 10):
    players = dal.session.execute(
        select(
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
        ).offset(skip).limit(limit)
    )

    players = [Player(first_name = fname, second_name = sname, position = pos) for fname, sname, pos in players]
    return players
