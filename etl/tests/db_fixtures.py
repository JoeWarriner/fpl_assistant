import pytest
import database.database as db 
from sqlalchemy.dialects.postgresql import insert


@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.reset_tables()
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.rollback()
    db.dal.session.close()


@pytest.fixture
def insert_seasons(database):
    db.dal.session.execute(
        insert(db.Season).values({
            db.Season.id: 1,
            db.Season.start_year: 2021,
            db.Season.season: '2021-22',
            db.Season.is_current: False
        }
    ))

    db.dal.session.execute(
        insert(db.Season).values({
            db.Season.id: 2,
            db.Season.start_year: 2022,
            db.Season.season: '2022-23',
            db.Season.is_current: False
        }
    )
    )