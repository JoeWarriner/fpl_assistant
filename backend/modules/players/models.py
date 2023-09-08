from pydantic import BaseModel

class Player(BaseModel):    
    id: int
    first_name: str
    second_name: str
    position: str
