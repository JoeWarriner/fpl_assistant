import pytest
import etl.update.database as db 
from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
import etl.update.api as api
import etl.update.extracters as extracters
import etl.update.adapters as adapters
import etl.update.loaders as loaders
import etl.update.tests.test_data.test_api_dicts as test_data
from etl.update.tests.utils import ProjectFilesForTests
from sqlalchemy import select, insert


@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.rollback()
    db.dal.session.close()

players = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Player, ProjectFilesForTests.player_overview_json),
        transformer= adapters.APITranformer(adapter=adapters.PlayerAdapter),
        loader = loaders.DBLoader(db.Player)
)




@pytest.fixture
def insert_season(database):
    db.dal.session.execute(
        insert(db.Season).values({
            db.Season.id: 2,
            db.Season.start_year: 2023,
            db.Season.season: '2023-24',
            db.Season.is_current: True
        }
    )
)


@pytest.fixture
def import_players(insert_season):
    orchestrator = PipelineOrchestrator()
    orchestrator.add_task(players)
    orchestrator.run()


def test_player_import(import_players):
    players = db.dal.session.execute(select(
        db.Player)
    ).all()
    assert len(players) == 4

    alisson, = db.dal.session.execute(select(db.Player).where(db.Player.second_name == 'Ramses Becker')).one()
    assert alisson.first_name == 'Alisson'
    assert alisson.fpl_id == 116535

    alvarez, = db.dal.session.execute(select(db.Player).where(db.Player.second_name == 'Álvarez')).one()
    assert alvarez.first_name == 'Julián'
    assert alvarez.fpl_id == 461358



# def test_team_import(database):
#     orchestrator = PipelineOrchestrator()
#     test_add_team = DataImportPipeline(
#         extracter= extracters.APIExtracter(api.Team, test_data.ARSENAL),
#         transformer= adapters.APITranformer(adapter=adapters.TeamAdapter),
#         loader = loaders.DBLoader(db.Team)
#     )
#     orchestrator.add_task(test_add_team)
#     orchestrator.run()

#     team = db.dal.session.scalar(select(db.Team))
#     assert team.team_name == 'Arsenal'
#     assert team.short_name == 'ARS'
#     assert team.fpl_id == 3







