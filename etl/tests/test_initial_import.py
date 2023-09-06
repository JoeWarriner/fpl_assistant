import pytest
import database.tables as tbl
from database.data_access_layer import dal
import etl.jobs.extractors.seasons_extractor as seasons_extractor
import etl.jobs.transformers.data_table_transformers as tform
from etl.jobs.loaders.loaders import DBLoader
import etl.jobs.extractors.api.api_models as api_models
from etl.jobs.extractors.data_table_extractor import DataTableExtractor
from etl.jobs.extractors.api_extractors import APIExtractor
from etl.pipeline_management.base_pipeline import Pipeline
from etl.pipeline_management.etl_pipeline import DataImportPipeline
from etl.jobs.transformers.api_transformers import APITransformer, PositionAdapter
from etl.utils.file_handlers import ProjectFiles
from etl.jobs.extractors.api.api_download import APIDownloader
from etl.tests.utils import PathsForTests
from pathlib import Path
from etl.imports.initial_import import  InitialImport

InitialImport.pathlib = PathsForTests
InitialImport.seasons_to_import =  [
        '2021-22',
        '2022-23'
    ]
initial_import = InitialImport()

from sqlalchemy import select
from sqlalchemy.orm import aliased





@pytest.fixture
def database():
    dal.conn_string = 'postgresql+psycopg2://postgres@localhost/fftest'
    dal.connect()
    dal.reset_tables() # Want to start from an empty database.
    dal.session = dal.Session()
    yield
    dal.session.rollback()
    dal.session.close()
    







@pytest.fixture
def seasons_import(database):
    orchestrator = Pipeline()
    orchestrator.add_task(initial_import.seasons)
    return orchestrator


def test_seasons_import(seasons_import):
    seasons_import.run()
    test_season = dal.session.scalar(select(tbl.Season).where(tbl.Season.start_year == 2022))
    assert test_season.season == '2022-23'
    all_seasons = dal.session.execute(select(tbl.Season)).all()
    assert len(all_seasons)  == 2


@pytest.fixture
def players_import(seasons_import: Pipeline):
    orchestrator = seasons_import
    orchestrator.add_task(initial_import.players, predecessors={initial_import.seasons})
    return orchestrator

def test_players_import(players_import):
    players_import.run()
    test_player = dal.session.scalar(select(tbl.Player).where(tbl.Player.first_name == 'Trent'))
    assert test_player.second_name == 'Alexander-Arnold'
    assert test_player.fpl_id == 169187
    all_players = dal.session.execute(select(tbl.Player)).all()
    assert len(all_players) == 3


@pytest.fixture
def teams_import(players_import: Pipeline):
    orchestrator = players_import
    orchestrator.add_task(initial_import.teams, predecessors={initial_import.seasons})
    return orchestrator

def test_teams_import(teams_import):
    teams_import.run()
    test_team = dal.session.scalar(select(tbl.Team).where(tbl.Team.short_name == 'LIV'))
    assert test_team.team_name == 'Liverpool'
    assert test_team.fpl_id == 14
    all_teams = dal.session.execute(select(tbl.Team)).all()
    assert len(all_teams) == 11

@pytest.fixture
def team_seasons_import(teams_import:Pipeline):
    orchestrator = teams_import
    orchestrator.add_task(initial_import.team_seasons, predecessors={initial_import.teams})
    return orchestrator

def test_team_seasons_import(team_seasons_import):
    team_seasons_import.run()
    test_team_season = dal.session.scalar(select(tbl.TeamSeason).join(tbl.TeamSeason.team).join(tbl.TeamSeason.season).where(tbl.Team.short_name == 'LIV' and tbl.Season.start_year == 2022))
    assert test_team_season.fpl_id == 12


@pytest.fixture
def positions_import(team_seasons_import: Pipeline):
    orchestrator = team_seasons_import
    orchestrator.add_task(initial_import.api_download)
    orchestrator.add_task(initial_import.positions, predecessors={initial_import.api_download})
    return orchestrator
    
def test_positions_import(positions_import):
    positions_import.run()
    test_position = dal.session.scalar(select(tbl.Position).where(tbl.Position.short_name == 'DEF'))
    assert test_position.pos_name == 'Defender'


@pytest.fixture
def player_seasons_import(positions_import: Pipeline):
    orchestrator = positions_import
    orchestrator.add_task(initial_import.player_seasons, predecessors={initial_import.players, initial_import.seasons, initial_import.positions})
    return orchestrator

def test_player_seasons_import(player_seasons_import):
    player_seasons_import.run()
    test_ps = dal.session.scalar(select(tbl.PlayerSeason).join(tbl.Season).join(tbl.Player).where(tbl.Player.first_name == 'Trent' and  tbl.Season.start_year == 2022))
    assert test_ps.fpl_id == 285


@pytest.fixture
def gameweeks_import(player_seasons_import: Pipeline):
    orchestrator = player_seasons_import
    orchestrator.add_task(initial_import.gameweeks, predecessors={initial_import.seasons})
    return orchestrator

def test_gameweeks_import(gameweeks_import):
    gameweeks_import.run()
    test_gw = dal.session.scalar(select(tbl.Gameweek).join(tbl.Season).where(tbl.Gameweek.gw_number == 1 and tbl.Season.start_year == 2022))
    assert test_gw


@pytest.fixture
def fixtures_import(gameweeks_import: Pipeline):
    orchestrator = gameweeks_import
    orchestrator.add_task(initial_import.fixtures, predecessors={initial_import.gameweeks, initial_import.seasons, initial_import.team_seasons})
    return orchestrator

def test_fixtures_import(fixtures_import):
    fixtures_import.run()
    test_fixture_home  = dal.session.scalar(
        select(
            tbl.Fixture
        ).join(
            tbl.Season
        ).join(
            tbl.Team, tbl.Fixture.home_team_id == tbl.Team.id
        ).where(
            tbl.Season.start_year == 2022 and tbl.Team.short_name == 'LIV'
            )
    )
    print(test_fixture_home.__dict__)
    assert test_fixture_home.fpl_code == 2292810
    assert test_fixture_home.fpl_id == 1

    test_fixture_away = dal.session.scalar(
        select(
            tbl.Fixture
        ).join(
            tbl.Season
        ).join(
            tbl.Team, tbl.Fixture.away_team_id == tbl.Team.id
        ).where(
            tbl.Season.start_year == 2022 and tbl.Team.short_name == 'ARS'
            )
    )

    assert test_fixture_away.fpl_code == 2292810
    assert test_fixture_away.fpl_id == 1


@pytest.fixture
def player_performances_import(fixtures_import: Pipeline):
    orchestrator = fixtures_import
    orchestrator.add_task(initial_import.player_performances, predecessors={initial_import.fixtures, initial_import.player_seasons})
    return orchestrator


def query_player_performance(gameweek, season, first_name):
    output  = dal.session.execute(
        select(
            tbl.Team.short_name,
            tbl.PlayerPerformance.was_home,
            tbl.PlayerPerformance.total_points
        ).join(
            tbl.PlayerPerformance, tbl.PlayerPerformance.opposition_id == tbl.Team.id
        ).join(
            tbl.Fixture, tbl.PlayerPerformance.fixture_id == tbl.Fixture.id
        ).join(
            tbl.Gameweek, tbl.Gameweek.id == tbl.Fixture.gameweek_id
        ).join(
            tbl.Season, tbl.Fixture.season_id == tbl.Season.id
        ).join(
            tbl.Player, tbl.PlayerPerformance.player_id == tbl.Player.id
        ).where(tbl.Season.season == season).where(tbl.Player.first_name == first_name).where(tbl.Gameweek.gw_number == gameweek)
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