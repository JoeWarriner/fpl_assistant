import pytest
from pathlib import Path
from database.data_access_layer import DataAccessLayer
from datetime import datetime
import database.tables as tbl
from sqlalchemy import insert, update
import pandas as pd

dal = DataAccessLayer()

test_data_path = Path(__file__).parent / 'test_data'

@pytest.fixture
def empty_database():
    dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    dal.connect()
    dal.reset_tables()
    dal.session = dal.Session()
    try:
        yield
    except Exception as e:
        dal.session.rollback()
        dal.session.close()
        raise Exception from e
    dal.session.close()


@pytest.fixture
def populated_database(empty_database):
    populate_database()
    yield



def populate_database():
    tables = (
        'seasons',
        'teams',
        'positions',
        'players',
        'team_seasons',
        'player_seasons',
        'gameweeks',
        'fixtures',
        'player_performances',
        'player_fixtures',
    )

    date_columns = {
        'gameweeks': 'deadline_time',
        'fixtures': 'kickoff_time'
    }
    for table in tables:
        if date_columns.get(table):
            table_data: pd.DataFrame = pd.read_csv(test_data_path / f'{table}_test.csv', parse_dates=[date_columns.get(table)], date_format="%d/%m/%Y")
        else:
            table_data: pd.DataFrame = pd.read_csv(test_data_path / f'{table}_test.csv') 

        if 'testing_notes' in table_data.columns:
            table_data = table_data.drop(columns=['testing_notes'])

        table_data.to_sql(table, dal.engine, index=False, if_exists='append')


@pytest.fixture
def populated_database_with_predictions(populated_database):
    dal.execute_transaction(update(tbl.PlayerFixture).values(
        predicted_score = 2.7
    )
    )
