from pydantic import BaseModel

class Player(BaseModel):    
    id: int
    first_name: str
    second_name: str
    current_value: int
    predicted_score: float
    position: str
    team: str 
        

