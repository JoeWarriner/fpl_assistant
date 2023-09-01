from sqlalchemy import create_engine, Column, Integer, Boolean, ForeignKey, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Gameweek(Base):
    __tablename__ = 'gameweeks'
    
    id = Column(Integer, primary_key=True)
    deadline_time = Column(TIMESTAMP)
    finished = Column(Boolean)
    is_previous = Column(Boolean)
    is_next = Column(Boolean)
    is_current = Column(Boolean)
    gw_number = Column(Integer)
    season = Column(Integer, ForeignKey('seasons.id'))
    
    season_rel = relationship("Season", back_populates="gameweeks")

    
