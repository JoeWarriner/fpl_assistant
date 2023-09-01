
from sqlalchemy import create_engine, Column, Integer, Boolean, String, TIMESTAMP, ForeignKey, text
from sqlalchemy.orm import relationship, declarative_base, sessionmaker, Session

Base = declarative_base()

class Season(Base):
    __tablename__ = 'seasons'
    
    id = Column(Integer, primary_key=True)
    is_current = Column(Boolean)
    start_year = Column(Integer, unique=True)
    season = Column(String(35))
    
    team_seasons = relationship("TeamSeason", back_populates="season")
    gameweeks = relationship("Gameweek", back_populates="season")
    player_seasons = relationship("PlayerSeason", back_populates="season")
    fixtures = relationship("Fixture", back_populates='season')

class Team(Base):
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer, unique=True)
    team_name = Column(String)
    short_name = Column(String)
    
    team_seasons = relationship("TeamSeason", back_populates="team")
    home_fixtures = relationship("Fixture", foreign_keys='Fixture.home_team_id', back_populates="home_team")
    away_fixtures = relationship("Fixture", foreign_keys='Fixture.away_team_id', back_populates="away_team")
    player_fixtures = relationship("PlayerFixture", back_populates="team")
    player_performances = relationship('PlayerPerformance', back_populates="team")
    opposition_player_performances = relationship('PlayerPerformance', back_populates="team")
    

class TeamSeason(Base):
    __tablename__ = 'team_seasons'
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer)
    team_id = Column(Integer, ForeignKey('teams.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))
    
    team = relationship("Team", back_populates="team_seasons")
    season = relationship("Season", back_populates="team_seasons")


class Gameweek(Base):
    __tablename__ = 'gameweeks'
    
    id = Column(Integer, primary_key=True)
    deadline_time = Column(TIMESTAMP)
    finished = Column(Boolean)
    is_previous = Column(Boolean)
    is_next = Column(Boolean)
    is_current = Column(Boolean)
    gw_number = Column(Integer)
    season_id = Column(Integer, ForeignKey('seasons.id'))
    
    season = relationship("Season", back_populates="gameweeks")
    fixtures = relationship("Fixture", back_populates="gameweek")

class Fixture(Base):
    __tablename__ = 'fixtures'
    
    id = Column(Integer, primary_key=True)
    gameweek_id = Column(Integer, ForeignKey('gameweeks.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_difficulty = Column(Integer)
    home_team_difficulty = Column(Integer)
    season_id = Column(Integer, ForeignKey('seasons.id'))
    away_team_score = Column(Integer)
    home_team_score = Column(Integer)
    fpl_id = Column(Integer)
    fpl_code = Column(Integer)
    kickoff_time = Column(TIMESTAMP)
    finished = Column(Boolean)
    started = Column(Boolean)
    
    gameweek = relationship("Gameweek", back_populates="fixtures")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_fixtures")
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_fixtures")
    player_fixtures = relationship("PlayerFixture", back_populates="fixture")
    player_performances = relationship("PlayerPerformance", back_populates="fixture")
    season = relationship("Season", back_populates="fixtures")

class PlayerPerformance(Base):
    __tablename__ = 'player_performances'
    
    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    opposition_id = Column(Integer)
    minutes_played = Column(Integer)
    penalties_missed = Column(Integer)
    penalties_saved = Column(Integer)
    red_cards = Column(Integer)
    yellow_cards = Column(Integer)
    selected = Column(Integer)
    total_points = Column(Integer)
    goals_scored = Column(Integer)
    goals_conceded = Column(Integer)
    clean_sheet = Column(Boolean)
    bonus = Column(Integer)
    assists = Column(Integer)
    was_home = Column(Boolean)
    
    fixture = relationship("Fixture", back_populates="player_performances")
    player = relationship("Player", back_populates="player_performances")
    team = relationship("Team", foreign_keys=[team_id], back_populates="player_performances")
    opposition = relationship("Team", foreign_keys=[team_id], back_populates="opposition_player_performances")

class PlayerFixture(Base):
    __tablename__ = 'player_fixtures'

    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    opposition_id = Column(Integer) 
    is_home = Column(Boolean)

    team = relationship('Team', back_populates="player_fixtures")
    fixture = relationship("Fixture", back_populates="player_fixtures")
    player = relationship("Player", back_populates="player_fixtures")



class PlayerSeason(Base):
    __tablename__ = 'player_seasons'
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer)
    new_col = Column(Integer)
    player_id = Column(Integer, ForeignKey('players.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))
    position_id = Column(Integer, ForeignKey('positions.id'))
    
    
    
    player = relationship("Player", back_populates="player_seasons")
    season = relationship("Season", back_populates="player_seasons")
    position = relationship("Position", back_populates="player_seasons")

class Player(Base):
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer, unique=True)
    first_name = Column(String(35))
    second_name = Column(String(35))
    
    player_performances = relationship("PlayerPerformance", back_populates="player")
    player_fixtures = relationship("PlayerFixture", back_populates="player")
    player_seasons = relationship("PlayerSeason", back_populates="player")

class Position(Base):
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer, unique=True)
    pos_name = Column(String(35))
    short_name = Column(String(5))

    player_seasons = relationship("PlayerSeason", back_populates='position')


MAIN_DB = 'postgresql+psycopg2://postgres@localhost/fantasyfootballassistant'
TEST_DB = 'postgresql+psycopg2://postgres@localhost/fftest'



class GenericDataAccessLayer:

    def __init__(self, connection: str):
        self.connection = connection

    def __enter__(self) -> Session:
        self.engine = create_engine(TEST_DB)
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind = self.engine)()

    def __exit__(self, *args): 
        pass

class FFADataAccessLayer(GenericDataAccessLayer):
    def __init__(self):
        self.connection = 'postgresql+psycopg2://postgres@localhost/fantasyfootballassistant'

class TestDataAccessLayer(GenericDataAccessLayer):
    def __init__(self):
        self.connection = 'postgresql+psycopg2://postgres@localhost/fftest'
    
    def __exit__(self, *args):
        Base.metadata.drop_all(self.engine)


class DataAccessLayer:
    session: Session

    def __init__(self): 
        self.engine = None
        self.conn_string = 'postgresql+psycopg2://postgres@localhost/fantasyfootballassistant'

    def connect(self): 
        self.engine = create_engine(self.conn_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


dal = DataAccessLayer() 

# with TestDataAccessLayer() as db:
#     print(db.execute(text('SELECT * FROM player_seasons;')))
#     db.commit()
    
# Create the engine and tables





# class Database:
#     metadata = MetaData()

#     def __new__(cls):
#         cls.engine = create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
#         fixtures_table(cls.metadata)
#         cls.metadata.create_all(cls.engine)


# Database()