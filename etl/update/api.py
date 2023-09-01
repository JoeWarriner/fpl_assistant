from pydantic import BaseModel
from datetime import datetime
from etl.update.utils.file_handlers import ProjectFiles
from etl.update.utils.paths import ProjectPaths
from typing import Optional



class GameWeek(BaseModel):
    id: int
    deadline_time: datetime
    finished: bool
    is_previous: bool
    is_current: bool
    is_next: bool

    @classmethod
    def extract(cls, api_data: dict):
        return cls.model_validate(ProjectFiles.gameweeks_json[0])



class Team(BaseModel):
    code: int
    id: int
    name: str
    short_name: str

class Player(BaseModel):
    chance_of_playing_next_round: Optional[int]
    chance_of_playing_this_round: Optional[int]
    code: int
    element_type: int
    first_name: str
    id: int
    now_cost: int
    second_name: str
    status: str
    team: int
    team_code: int

class Position(BaseModel):
    id: int
    singular_name: str
    singular_name_short: str

class Summary(BaseModel):
    events: list[GameWeek]
    teams: list[Team]
    elements: list[Player]
    element_types: list[Position]
        

class Fixture(BaseModel):
    code: int
    event: Optional[int]
    finished: bool
    id: int
    kickoff_time: datetime
    minutes: int
    started: Optional[bool]
    team_a: int
    team_a_score: Optional[int]
    team_h: int
    team_h_score: Optional[int]
    team_h_difficulty: int
    team_a_difficulty: int


class PlayerFixture(BaseModel):
    id: int
    code: int
    team_h: int
    team_h_score: int
    team_a: int
    team_a_score: int
    event: int
    finished: bool
    minutes: int
    provisional_start_time: bool
    kickoff_time: str
    event_name: str
    is_home: bool
    difficulty: int  

class PlayerPerformance(BaseModel):
    element: int
    fixture: int
    opponent_team: int
    total_points: int
    was_home: bool
    kickoff_time: str
    team_h_score: int
    team_a_score: int
    round: int
    minutes: int
    goals_scored: int
    assists: int
    clean_sheets: int
    goals_conceded: int
    own_goals: int
    penalties_saved: int
    penalties_missed: int
    yellow_cards: int
    red_cards: int
    saves: int
    bonus: int
    bps: int
    influence: str
    creativity: str
    threat: str
    ict_index: str
    starts: int
    expected_goals: str
    expected_assists: str
    expected_goal_involvements: str
    expected_goals_conceded: str
    value: int
    transfers_balance: int
    selected: int
    transfers_in: int
    transfers_out: int

class PlayerDetails(BaseModel):
    fixtures: list[PlayerFixture]
    history: list[PlayerPerformance]

