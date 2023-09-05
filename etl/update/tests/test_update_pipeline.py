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


players = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Player, ProjectFilesForTests.player_overview_json),
        transformer= adapters.APITranformer(adapter=adapters.PlayerAdapter),
        loader = loaders.DBLoader(db.Player)
)

teams = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Team, ProjectFilesForTests.teams_json),
        transformer= adapters.APITranformer(adapter=adapters.TeamAdapter),
        loader = loaders.DBLoader(db.Team)
)


positions = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Position, ProjectFilesForTests.positions_json),
        transformer= adapters.APITranformer(adapter=adapters.PositionAdapter),
        loader = loaders.DBLoader(db.Position)
)

player_seasons = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Player, ProjectFilesForTests.player_overview_json),
        transformer= adapters.APITranformer(adapter=adapters.PlayerSeason),
        loader = loaders.DBLoader(db.PlayerSeason)
)

team_seasons = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Team, ProjectFilesForTests.teams_json),
        transformer= adapters.APITranformer(adapter=adapters.TeamSeasonAdapter),
        loader = loaders.DBLoader(db.TeamSeason)
)


gameweeks = DataImportPipeline(
        extracter= extracters.APIExtracter(api.GameWeek, ProjectFilesForTests.gameweeks_json),
        transformer= adapters.APITranformer(adapter=adapters.GameWeekAdapter),
        loader = loaders.DBLoader(db.Gameweek)
)

fixtures = DataImportPipeline(
        extracter= extracters.APIExtracter(api.Fixture, ProjectFilesForTests.fixtures_json),
        transformer= adapters.APITranformer(adapter=adapters.TeamSeasonAdapter),
        loader = loaders.DBLoader(db.TeamSeason)
)

player_fixtures = DataImportPipeline(
        extracter= extracters.APIExtracter(api.PlayerFixture, ProjectFilesForTests.get_player_detail_json),
        transformer= adapters.APITranformer(adapter=adapters.PlayerFixtureAdapter),
        loader = loaders.DBLoader(db.PlayerFixture)
)


player_performances = DataImportPipeline(
        extracter= extracters.APIExtracter(api.PlayerPerformance, ProjectFilesForTests.get_player_detail_json),
        transformer= adapters.APITranformer(adapter=adapters.PlayerPerformanceAdapter),
        loader = loaders.DBLoader(db.PlayerPerformance)
)



@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.rollback()
    db.dal.session.close()




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
    return orchestrator




def test_player_import(import_players):
    import_players.run()
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


@pytest.fixture
def import_teams(import_players):
    orchestrator = import_players
    orchestrator.add_task(teams)
    return orchestrator

def test_team_import(import_teams):
    import_teams.run()
    teams = db.dal.session.execute(select(
        db.Team)
    ).all()
    assert len(teams) == 4

    liverpool, = db.dal.session.execute(select(db.Team).where(db.Team.short_name == 'LIV')).one()
    assert liverpool.team_name == 'Liverpool'
    assert liverpool.fpl_id == 14

    city, = db.dal.session.execute(select(db.Team).where(db.Team.short_name == 'MCI')).one()
    assert city.team_name == 'Man City'
    assert city.fpl_id == 43


@pytest.fixture
def import_positions(import_teams):
    orchestrator = import_teams
    orchestrator.add_task(positions)
    return orchestrator

def test_position_import(import_positions):
    import_positions.run()
    positions = db.dal.session.execute(select(
        db.Position)
    ).all()
    assert len(positions) == 4

    goalkeeper, = db.dal.session.execute(select(db.Position).where(db.Position.short_name == 'GKP')).one()
    assert goalkeeper.fpl_id == 1
    assert goalkeeper.pos_name == 'Goalkeeper'

    forward, = db.dal.session.execute(select(db.Position).where(db.Position.short_name == 'FWD')).one()
    assert forward.pos_name == 'Forward'
    assert forward.fpl_id == 4


@pytest.fixture
def import_team_seasons(import_positions):
    orchestrator = import_positions
    orchestrator.add_task(team_seasons, predecessors={teams})
    return orchestrator


def team_season_test_query(fpl_id, season):
    output, = db.dal.session.execute(
        select(
            db.Team.short_name
        ).select_from(
            db.TeamSeason
        ).join(
            db.Team, db.Team.id == db.TeamSeason.team_id
        ).join(
            db.Season, db.Season.id == db.TeamSeason.season_id
        ).where(
            db.TeamSeason.fpl_id == fpl_id , db.Season.season == season
        )
    ).one()
    return output

def test_team_season_import(import_team_seasons):
    import_team_seasons.run()
    team_seasons = db.dal.session.execute(select(
        db.TeamSeason)
    ).all()
    assert len(team_seasons) == 4

    liverpool_2023_short_name = team_season_test_query(11, '2023-24')
    assert liverpool_2023_short_name == 'LIV'
    
    city_2023_short_name = team_season_test_query(13, '2023-24')
    assert city_2023_short_name == 'MCI'









