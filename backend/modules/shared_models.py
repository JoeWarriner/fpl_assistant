from pydantic import BaseModel
from typing import Union

class Player(BaseModel):    
    id: int
    first_name: str
    second_name: str
    current_value: int
    predicted_score: Union[float, None]
    position: str
    team: str 
        

