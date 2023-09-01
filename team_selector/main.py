from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI()

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


