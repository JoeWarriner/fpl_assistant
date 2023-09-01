import pytest
import etl.update.database as db 
from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
import etl.update.api as api
import etl.update.extracters as extracters
import etl.update.adapters as adapters
import etl.update.loaders as loaders
import etl.update.tests.test_data_dicts as test_data
from sqlalchemy import select


@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.rollback()
    db.dal.session.close()


def test_player_import(database):
    
    orchestrator = PipelineOrchestrator()
    test_add_player = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Player, test_data.MO_SALAH),
        transformer= adapters.PlayerAdapter(),
        loader = loaders.DBLoader(db.Player)
    )
    orchestrator.add_task(test_add_player)
    orchestrator.run()

    player = db.dal.session.scalar(select(
        db.Player
        ))
        
    assert player.first_name == 'Mohamed'
    assert player.second_name == 'Salah'
    assert player.fpl_id == 118748
    

def test_team_import(database):
    orchestrator = PipelineOrchestrator()
    test_add_team = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Team, test_data.ARSENAL),
        transformer= adapters.TeamAdapter(),
        loader = loaders.DBLoader(db.Team)
    )
    orchestrator.add_task(test_add_team)
    orchestrator.run()

    team = db.dal.session.scalar(select(db.Team))
    assert team.team_name == 'Arsenal'
    assert team.short_name == 'ARS'
    assert team.fpl_id == 3
    




