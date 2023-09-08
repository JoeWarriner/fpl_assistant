import pytest
from database.data_access_layer import dal
import database.tables as tbl 
from sqlalchemy.dialects.postgresql import insert


@pytest.fixture
def database():
    dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    dal.connect()
    dal.reset_tables()
    dal.session = dal.Session()
    yield
    dal.session.rollback()
    dal.session.close()


@pytest.fixture
def insert_seasons(database):
    dal.session.execute(
        insert(tbl.Season).values({
            tbl.Season.id: 1,
            tbl.Season.start_year: 2021,
            tbl.Season.season: '2021-22',
            tbl.Season.is_current: False
        }
    ))

    dal.session.execute(
        insert(tbl.Season).values({
            tbl.Season.id: 2,
            tbl.Season.start_year: 2022,
            tbl.Season.season: '2022-23',
            tbl.Season.is_current: False
        }
    )
    )