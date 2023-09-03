import pytest
import etl.update.database as db
import etl.seed_db.seed as seed
from sqlalchemy import select

@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.close()
    db.dal.drop_tables()
    

def test_create_seasons(database):
    seed.extract_seasons()
    output = db.dal.session.scalar(select(db.Season).where(db.Season.is_current == True))
    assert output.start_year == 2022