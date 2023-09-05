import pytest
import etl.update.database as db
import etl.seed_db.seed as seed
import etl.seed_db.data_files as tform
from etl.update.loaders import DBLoader
import etl.update.api as api
from etl.update.extracters import DataTableExtracter, APIExtracter
from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
from etl.update.adapters import APITranformer, PositionAdapter
from etl.update.utils.file_handlers import ProjectFiles
from etl.update.extract_api_data import APIDownloader
from etl.update.tests.utils import PathsForTests
from pathlib import Path


from sqlalchemy import select
from sqlalchemy.orm import aliased

SEASONS_TO_IMPORT = [
        '2021-22',
        '2022-23'
    ]

@pytest.fixture
def database():
    db.dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    db.dal.connect()
    db.dal.reset_tables() # Want to start from an empty database.
    db.dal.session = db.dal.Session()
    yield
    db.dal.session.rollback()
    db.dal.session.close()
    

def test_is_current_season_aug_start_year():
    season_creater = seed.CreateSeasons(seasons=None)
    season_creater.now_month = 8
    season_creater.now_year = 2019
    assert not season_creater.season_is_current(season_start_year=2018)
    assert season_creater.season_is_current(season_start_year=2019)
    assert not season_creater.season_is_current(season_start_year=2020)

def test_is_current_season_jul_end_year():
    season_creater = seed.CreateSeasons(seasons=None)
    season_creater.now_month = 7
    season_creater.now_year = 2019
    assert not season_creater.season_is_current(season_start_year=2017)
    assert season_creater.season_is_current(season_start_year=2018)
    assert not season_creater.season_is_current(season_start_year=2019)

# def test_create_seasons(database):
#     seasons = DataImportPipeline(
#         extracter=seed.CreateSeasons(SEASONS_TO_IMPORT),
#         transformer=None,
#         loader= DBLoader(db.Season)
#     )
#     seasons.run()
#     season_2021 = db.dal.session.scalar(select(db.Season).where(db.Season.start_year == 2021))
#     assert season_2021.season == '2021-22'
#     all_seasons = db.dal.session.execute(select(db.Season)).all()
#     assert len(all_seasons) == 5

seasons = DataImportPipeline(
        extracter=seed.CreateSeasons(SEASONS_TO_IMPORT),
        transformer=None,
        loader= DBLoader(db.Season)
)

players = DataImportPipeline(
        extracter = DataTableExtracter(SEASONS_TO_IMPORT, 'players_raw.csv', pathlib = PathsForTests),
        transformer = tform.PlayerTransformer(),
        loader =  DBLoader(db.Player)
    )

teams = DataImportPipeline(
    extracter= DataTableExtracter(SEASONS_TO_IMPORT, 'teams.csv', pathlib = PathsForTests),
    transformer= tform.TeamTransformer(),
    loader = DBLoader(db.Team)
)

team_seasons = DataImportPipeline(
    extracter= DataTableExtracter(SEASONS_TO_IMPORT, 'players_raw.csv', pathlib = PathsForTests),
    transformer= tform.TeamSeasonTransformer(),
    loader=DBLoader(db.TeamSeason)
)

positions = DataImportPipeline(
    extracter = APIExtracter(api.Position, ProjectFiles.positions_json),
    transformer = APITranformer(PositionAdapter),
    loader = DBLoader(db.Position)
)

player_seasons = DataImportPipeline(
    extracter= DataTableExtracter(SEASONS_TO_IMPORT, 'players_raw.csv', pathlib = PathsForTests),
    transformer=tform.PlayerSeasonTransformer(),
    loader = DBLoader(db.PlayerSeason)
)

gameweeks = DataImportPipeline(
    extracter = DataTableExtracter(SEASONS_TO_IMPORT, Path('gws', 'merged_gw.csv'), pathlib = PathsForTests),
    transformer= tform.GameWeekTransformer(),
    loader = DBLoader(db.Gameweek)
)

fixtures = DataImportPipeline(
    extracter = DataTableExtracter(SEASONS_TO_IMPORT, 'fixtures.csv', pathlib = PathsForTests),
    transformer=tform.FixturesTransformer(),
    loader=DBLoader(db.Fixture)
)

player_performances = DataImportPipeline(
    extracter=DataTableExtracter(SEASONS_TO_IMPORT, Path('gws', 'merged_gw.csv'), pathlib = PathsForTests),
    transformer= tform.PlayerPerformanceTransformer(),
    loader= DBLoader(db.PlayerPerformance)
)

api_download = APIDownloader()




@pytest.fixture
def seasons_import(database):
    orchestrator = PipelineOrchestrator()
    orchestrator.add_task(seasons)
    return orchestrator


def test_seasons_import(seasons_import):
    seasons_import.run()
    test_season = db.dal.session.scalar(select(db.Season).where(db.Season.start_year == 2022))
    assert test_season.season == '2022-23'
    all_seasons = db.dal.session.execute(select(db.Season)).all()
    assert len(all_seasons)  == len(SEASONS_TO_IMPORT)


@pytest.fixture
def players_import(seasons_import: PipelineOrchestrator):
    orchestrator = seasons_import
    orchestrator.add_task(players, predecessors={seasons})
    return orchestrator

def test_players_import(players_import):
    players_import.run()
    test_player = db.dal.session.scalar(select(db.Player).where(db.Player.first_name == 'Trent'))
    assert test_player.second_name == 'Alexander-Arnold'
    assert test_player.fpl_id == 169187
    all_players = db.dal.session.execute(select(db.Player)).all()
    assert len(all_players) == 3


@pytest.fixture
def teams_import(players_import: PipelineOrchestrator):
    orchestrator = players_import
    orchestrator.add_task(teams, predecessors={seasons})
    return orchestrator

def test_teams_import(teams_import):
    teams_import.run()
    test_team = db.dal.session.scalar(select(db.Team).where(db.Team.short_name == 'LIV'))
    assert test_team.team_name == 'Liverpool'
    assert test_team.fpl_id == 14
    all_teams = db.dal.session.execute(select(db.Team)).all()
    assert len(all_teams) == 11

@pytest.fixture
def team_seasons_import(teams_import:PipelineOrchestrator):
    orchestrator = teams_import
    orchestrator.add_task(team_seasons, predecessors={teams})
    return orchestrator

def test_team_seasons_import(team_seasons_import):
    team_seasons_import.run()
    test_team_season = db.dal.session.scalar(select(db.TeamSeason).join(db.TeamSeason.team).join(db.TeamSeason.season).where(db.Team.short_name == 'LIV' and db.Season.start_year == 2022))
    assert test_team_season.fpl_id == 12


@pytest.fixture
def positions_import(team_seasons_import: PipelineOrchestrator):
    orchestrator = team_seasons_import
    orchestrator.add_task(api_download)
    orchestrator.add_task(positions, predecessors={api_download})
    return orchestrator
    
def test_positions_import(positions_import):
    positions_import.run()
    test_position = db.dal.session.scalar(select(db.Position).where(db.Position.short_name == 'DEF'))
    assert test_position.pos_name == 'Defender'


@pytest.fixture
def player_seasons_import(positions_import: PipelineOrchestrator):
    orchestrator = positions_import
    orchestrator.add_task(player_seasons, predecessors={players, seasons, positions})
    return orchestrator

def test_player_seasons_import(player_seasons_import):
    player_seasons_import.run()
    test_ps = db.dal.session.scalar(select(db.PlayerSeason).join(db.Season).join(db.Player).where(db.Player.first_name == 'Trent' and  db.Season.start_year == 2022))
    assert test_ps.fpl_id == 285


@pytest.fixture
def gameweeks_import(player_seasons_import: PipelineOrchestrator):
    orchestrator = player_seasons_import
    orchestrator.add_task(gameweeks, predecessors={seasons})
    return orchestrator

def test_gameweeks_import(gameweeks_import):
    gameweeks_import.run()
    test_gw = db.dal.session.scalar(select(db.Gameweek).join(db.Season).where(db.Gameweek.gw_number == 1 and db.Season.start_year == 2022))
    assert test_gw


@pytest.fixture
def fixtures_import(gameweeks_import: PipelineOrchestrator):
    orchestrator = gameweeks_import
    orchestrator.add_task(fixtures, predecessors={gameweeks, seasons, team_seasons})
    return orchestrator

def test_fixtures_import(fixtures_import):
    fixtures_import.run()
    test_fixture_home  = db.dal.session.scalar(
        select(
            db.Fixture
        ).join(
            db.Season
        ).join(
            db.Team, db.Fixture.home_team_id == db.Team.id
        ).where(
            db.Season.start_year == 2022 and db.Team.short_name == 'LIV'
            )
    )
    print(test_fixture_home.__dict__)
    assert test_fixture_home.fpl_code == 2292810
    assert test_fixture_home.fpl_id == 1

    test_fixture_away = db.dal.session.scalar(
        select(
            db.Fixture
        ).join(
            db.Season
        ).join(
            db.Team, db.Fixture.away_team_id == db.Team.id
        ).where(
            db.Season.start_year == 2022 and db.Team.short_name == 'ARS'
            )
    )

    assert test_fixture_away.fpl_code == 2292810
    assert test_fixture_away.fpl_id == 1


@pytest.fixture
def player_performances_import(fixtures_import: PipelineOrchestrator):
    orchestrator = fixtures_import
    orchestrator.add_task(player_performances, predecessors={fixtures, player_seasons})
    return orchestrator


def query_player_performance(gameweek, season, first_name):
    output  = db.dal.session.execute(
        select(
            db.Team.short_name,
            db.PlayerPerformance.was_home,
            db.PlayerPerformance.total_points
        ).join(
            db.PlayerPerformance, db.PlayerPerformance.opposition_id == db.Team.id
        ).join(
            db.Fixture, db.PlayerPerformance.fixture_id == db.Fixture.id
        ).join(
            db.Gameweek, db.Gameweek.id == db.Fixture.gameweek_id
        ).join(
            db.Season, db.Fixture.season_id == db.Season.id
        ).join(
            db.Player, db.PlayerPerformance.player_id == db.Player.id
        ).where(db.Season.season == season).where(db.Player.first_name == first_name).where(db.Gameweek.gw_number == gameweek)
    ).one()
    return output



def test_player_performances_import(player_performances_import):
    player_performances_import.run()
    performance_1 = query_player_performance(1, '2022-23', 'Trent')
    assert performance_1.short_name == 'ARS'
    assert performance_1.was_home == True
    assert performance_1.total_points == 1

    performance_2 = query_player_performance(2, '2021-22', 'Emiliano')
    assert performance_2.short_name == 'MCI'
    assert performance_2.was_home == True
    assert performance_2.total_points == 5