
from sqlalchemy import Column, Integer, Boolean, String, TIMESTAMP, ForeignKey,  UniqueConstraint, Float
from sqlalchemy.orm import relationship, DeclarativeBase

class Base(DeclarativeBase):
    pass

class Season(Base):
    __tablename__ = 'seasons'
    __table_args__ = (UniqueConstraint('start_year',  name='seasons__prevent_duplicate_import'),)

    id = Column(Integer, primary_key=True)
    is_current = Column(Boolean)
    start_year = Column(Integer)
    season = Column(String(35))
    
    team_seasons = relationship("TeamSeason", back_populates="season")
    gameweeks = relationship("Gameweek", back_populates="season")
    player_seasons = relationship("PlayerSeason", back_populates="season")
    fixtures = relationship("Fixture", back_populates='season')


class Team(Base):
    __tablename__ = 'teams'
    __table_args__ = (UniqueConstraint('fpl_id',  name='teams__prevent_duplicate_import'),)


    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer)
    team_name = Column(String)
    short_name = Column(String)
    
    
    team_seasons = relationship("TeamSeason", back_populates="team")

    

class TeamSeason(Base):
    __tablename__ = 'team_seasons'
    __table_args__ = (UniqueConstraint('fpl_id', 'season_id', name='team_seasons__prevent_duplicate_import'),)

    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer)
    team_id = Column(Integer, ForeignKey('teams.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))
    
    team = relationship("Team", back_populates="team_seasons")
    season = relationship("Season", back_populates="team_seasons")


class Gameweek(Base):
    __tablename__ = 'gameweeks'
    __table_args__ = (UniqueConstraint('gw_number', 'season_id', name='gameweeks__prevent_duplicate_import'), )
    
    id = Column(Integer, primary_key=True)
    deadline_time = Column(TIMESTAMP)
    finished = Column(Boolean)
    is_previous = Column(Boolean)
    is_next = Column(Boolean)
    is_current = Column(Boolean)
    gw_number = Column(Integer, nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable= False)
    
    season = relationship("Season", back_populates="gameweeks")
    fixtures = relationship("Fixture", back_populates="gameweek")

class Fixture(Base):
    __tablename__ = 'fixtures'
    __table_args__ = (UniqueConstraint('fpl_code', name='fixtures__prevent_duplicate_import'), )

    
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
    fpl_code = Column(Integer, unique=True)
    kickoff_time = Column(TIMESTAMP)
    finished = Column(Boolean)
    started = Column(Boolean)
    
    gameweek = relationship("Gameweek", back_populates="fixtures")
    away_team = relationship("Team", foreign_keys=away_team_id, backref="away_fixtures")
    home_team = relationship("Team", foreign_keys=home_team_id, backref="home_fixtures")

    season = relationship("Season", back_populates="fixtures")

class PlayerPerformance(Base):
    __tablename__ = 'player_performances'
    __table_args__ = (
        UniqueConstraint('fixture_id', 'player_id', name = 'player_performances__prevent_duplicate_import'),
    )

    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    opposition_id = Column(Integer, ForeignKey('teams.id'))
    minutes_played = Column(Integer)
    penalties_missed = Column(Integer)
    penalties_saved = Column(Integer)
    player_value = Column(Integer)
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
    difficulty = Column(Integer)
    saves = Column(Integer)
    expected_goals = Column(Float)
    expected_assists = Column(Float)
    expected_goals_conceded = Column(Float)
    influence = Column(Float)
    creativity =  Column(Float)
    threat = Column(Float)
    bps = Column(Integer)
    


    
    fixture = relationship("Fixture", backref="player_performances")
    player = relationship("Player", back_populates="player_performances")

    team = relationship("Team", foreign_keys=team_id, backref="player_performances")
    opposition = relationship("Team", foreign_keys=opposition_id, backref="opposition_player_performances")

class PlayerFixture(Base):
    __tablename__ = 'player_fixtures'
    __table_args__ = (
        UniqueConstraint('fixture_id', 'player_id', name = 'player_fixtures__prevent_duplicate_import'),
    )
    id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    opposition_id = Column(Integer, ForeignKey('teams.id')) 
    predicted_score = Column(Float, nullable=True)
    difficulty = Column(Integer)
    is_home = Column(Boolean)

    team = relationship('Team', foreign_keys=team_id, backref="team_player_fixtures")
    opposition = relationship('Team', foreign_keys=opposition_id, backref='opposition_player_fixtures')


    fixture = relationship("Fixture", backref="player_fixtures")
    player = relationship("Player", back_populates="player_fixtures")



class PlayerSeason(Base):
    __tablename__ = 'player_seasons'
    __table_args__ = (
        UniqueConstraint('fpl_id', 'season_id', name = 'player_seasons__prevent_duplicate_import'),
    )
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer)
    player_id = Column(Integer, ForeignKey('players.id'))
    season_id = Column(Integer, ForeignKey('seasons.id'))
    position_id = Column(Integer, ForeignKey('positions.id'))
    
    player = relationship("Player", back_populates="player_seasons")
    season = relationship("Season", back_populates="player_seasons")
    position = relationship("Position", back_populates="player_seasons")

class Player(Base):
    __tablename__ = 'players'

    __table_args__ = (
        UniqueConstraint('fpl_id', name = 'players__prevent_duplicate_import'),
    )
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer, unique=True)
    first_name = Column(String(35))
    second_name = Column(String(35))
    current_value = Column(Integer, nullable=True)
    
    player_performances = relationship("PlayerPerformance", back_populates="player")
    player_fixtures = relationship("PlayerFixture", back_populates="player")
    player_seasons = relationship("PlayerSeason", back_populates="player")

class Position(Base):
    __tablename__ = 'positions'
    __table_args__ = (
        UniqueConstraint('fpl_id', name = 'positions__prevent_duplicate_import'),
    )
    
    id = Column(Integer, primary_key=True)
    fpl_id = Column(Integer, unique=True)
    pos_name = Column(String(35))
    short_name = Column(String(5))

    player_seasons = relationship("PlayerSeason", back_populates='position')

