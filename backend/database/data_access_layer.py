from typing import Any
from typing_extensions import SupportsIndex
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from database.tables import Base

class DataAccessLayer:
    session: Session

    def __init__(self): 
        self.engine = None
        self.conn_string = 'postgresql+psycopg2://postgres@localhost/fantasyfootballassistant'

    def connect(self): 
        self.engine = create_engine(self.conn_string)
        self.create_tables()
        self.Session = sessionmaker(bind=self.engine)
    
    def reset_tables(self):
        self.drop_tables()
        self.create_tables()

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def execute_transaction(self, statement):   
        """Execute single statement as transaction."""
        try:
            output = self.session.execute(statement)
        except Exception as e:
            self.session.rollback()
            raise Exception from e
        else:
            self.session.commit()
            return output
    
    def execute_scalars(self, statement):   
        """Execute single statement as transaction."""
        try:
            output = self.session.scalars(statement)
        except Exception as e:
            self.session.rollback()
            raise Exception from e
        else:
            self.session.commit()
            return output
    
    def execute_scalar(self, statement):
        try:
            output = self.session.scalar(statement)
        except Exception as e:
            self.session.rollback()
            raise Exception from e
        else:
            self.session.commit()
            return output

     


dal = DataAccessLayer() 

