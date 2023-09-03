import pytest
import etl.update.database as db
import etl.seed_db.seed as seed
import etl.seed_db.data_files as tform
from etl.update.loaders import DBLoader
from etl.update.extracters import DataTableExtracter
from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline

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
    orchestrator = PipelineOrchestrator()
    
    seasons_with_data = [
        '2018-19',
        '2019-20',
        '2020-21',
        '2021-22',
        '2022-23'
    ]

    seasons = seed.CreateSeasons()

    players = DataImportPipeline(
        extracter = DataTableExtracter(seasons_with_data, 'players_raw.csv'),
        transformer = tform.PlayerTransformer(),
        loader =  DBLoader(db.Player)
    )

    orchestrator.add_task(players, predecessors={seasons})
    orchestrator.add_task(seasons)
    orchestrator.run()

    player = db.dal.session.scalar(select(
        db.Player
        ).where(db.Player.fpl_id == 118748))
    
    assert player.first_name == 'Mohamed'
    assert player.second_name == 'Salah'


    # output = db.dal.session.scalar(select(db.Season).where(db.Season.is_current == True))
    # assert output.start_year == 2022    