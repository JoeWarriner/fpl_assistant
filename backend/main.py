from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database.data_access_layer import DataAccessLayer

import modules.players.router as players
import modules.team_selector.router as team_selector

app = FastAPI()
dal = DataAccessLayer()
dal.connect()
dal.session = dal.Session()

app.add_middleware(CORSMiddleware, allow_origins=['*'])
app.include_router(players.router)
app.include_router(team_selector.router)




