from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database.data_access_layer import dal
import database.tables as tbl
from typing import Optional
from pydantic import BaseModel
from sqlalchemy import select

import modules.players.router as players
import json

app = FastAPI()

dal.connect()
dal.session = dal.Session()

app.add_middleware(CORSMiddleware, allow_origins=['*'])
app.include_router(players.router)

@app.get('/best-team')
async def best_team():
    '''
    Get an optimised team:
    http://localhost:8000/best-team
    '''
    
    with open('data/current_best_team.json') as file:
        team = json.loads(file.read())
        
    return team




