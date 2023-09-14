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
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def reset_tables(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def execute(self, statement):   
        """Execute single statement as transaction."""
        try:
            output = self.session.execute(statement)
        except Exception as e:
            self.session.rollback()
            raise Exception from e
        else:
            self.session.commit()
            return output
        



dal = DataAccessLayer() 

