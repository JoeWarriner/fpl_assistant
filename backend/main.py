from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database.data_access_layer import DataAccessLayer
import database.tables as tbl
from typing import Optional
from pydantic import BaseModel
from sqlalchemy import select

import modules.players.router as players
import modules.team_selector.router as team_selector
import json

app = FastAPI()
dal = DataAccessLayer()
dal.connect()
dal.session = dal.Session()

app.add_middleware(CORSMiddleware, allow_origins=['*'])
app.include_router(players.router)
app.include_router(team_selector.router)




