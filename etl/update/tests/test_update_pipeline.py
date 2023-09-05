import pytest
from datetime import datetime
import etl.update.database as db 
from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
import etl.update.api as api
import etl.update.extracters as extracters
import etl.update.adapters as adapters
import etl.update.loaders as loaders
import etl.update.tests.test_data.test_api_dicts as test_data
from etl.update.tests.utils import ProjectFilesForTests
from sqlalchemy import select, insert
from sqlalchemy.orm import aliased


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
        transformer= adapters.APITranformer(adapter=adapters.FixtureAdapter),
        loader = loaders.DBLoader(db.Fixture)
)

player_fixtures = DataImportPipeline(
        extracter= extracters.APIExtracter(api.PlayerFixture, ProjectFilesForTests.get_all_player_fixtures),
        transformer= adapters.APITranformer(adapter=adapters.PlayerFixtureAdapter),
        loader = loaders.DBLoader(db.PlayerFixture)
)


player_performances = DataImportPipeline(
        extracter= extracters.APIExtracter(api.PlayerPerformance, ProjectFilesForTests.get_all_player_performances),
        transformer= adapters.APITranformer(adapter=adapters.PlayerPerformanceAdapter),
        loader = loaders.DBLoader(db.PlayerPerformance)
)



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
            db.TeamSeason.fpl_id == fpl_id
        ).where(
            db.Season.season == season
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



@pytest.fixture
def import_player_seasons(import_team_seasons):
    orchestrator = import_team_seasons
    orchestrator.add_task(player_seasons, predecessors={players, team_seasons, positions})
    return orchestrator

def player_season_test_query(first_name, season):
    second_name, short_name, fpl_id = db.dal.session.execute(
        select(
            db.Player.second_name, db.Position.short_name, db.PlayerSeason.fpl_id,
        ).select_from(
            db.PlayerSeason
        ).join(
            db.Season, db.Season.id == db.PlayerSeason.season_id
        ).join(
            db.Player, db.Player.id == db.PlayerSeason.player_id
        ).join(
            db.Position, db.Position.id == db.PlayerSeason.position_id
        ).where(
            db.Player.first_name == first_name
        ).where(
            db.Season.season == season
        )
        ).one()
    return second_name, short_name, fpl_id



def test_player_season_import(import_player_seasons):
    import_player_seasons.run()

    player_seasons = db.dal.session.scalars(select(
        db.PlayerSeason)
    ).all()
    for player_season in player_seasons:
        print(player_season.__dict__)
    assert len(player_seasons) == 4

    second_name, short_name, fpl_id = player_season_test_query('Alisson', '2023-24')
    assert second_name == 'Ramses Becker'
    assert short_name == 'GKP'
    assert fpl_id == 291
    
    second_name, short_name, fpl_id = player_season_test_query('Julián', '2023-24')
    assert second_name == 'Álvarez'
    assert short_name == 'FWD'
    assert fpl_id == 343




@pytest.fixture
def import_gameweeks(import_player_seasons):
    orchestrator = import_player_seasons
    orchestrator.add_task(gameweeks)
    return orchestrator

def gamweek_test_query(gw_number, season):
    gameweek, = db.dal.session.execute(
        select(
            db.Gameweek
        ).join(
            db.Season, db.Season.id == db.Gameweek.season_id
        ).where(
            db.Season.season == season
        ).where(
            db.Gameweek.gw_number == gw_number
        )
        ).one()
    return gameweek


def test_gameweek_import(import_gameweeks):
    import_gameweeks.run()

    gameweeks = db.dal.session.scalars(select(
        db.Gameweek)
    ).all()
    assert len(gameweeks) == 2

    gameweek_1 = gamweek_test_query(1, '2023-24')
    assert gameweek_1.deadline_time == datetime(2023,8,11,18,30)
    assert gameweek_1.finished == True
    assert gameweek_1.is_previous == False
    assert gameweek_1.is_current == False
    assert gameweek_1.is_next == False
    
    gameweek_5 = gamweek_test_query(5, '2023-24')
    assert gameweek_5.deadline_time == datetime(2023,9,16,11)
    assert gameweek_5.finished == False
    assert gameweek_5.is_previous == False
    assert gameweek_5.is_current == False
    assert gameweek_5.is_next == True
    


@pytest.fixture
def import_fixtures(import_gameweeks):
    orchestrator = import_gameweeks
    orchestrator.add_task(fixtures, predecessors = {gameweeks, team_seasons})
    return orchestrator


def fixture_test_query(season, home, away):
    away_team = aliased(db.Team, name='away_team')
    home_team = aliased(db.Team, name='home_team')
    fixture, = db.dal.session.execute(
        select(
            db.Fixture
        ).join(
            db.Season, db.Season.id == db.Fixture.season_id
        ).join(
            away_team, db.Fixture.away_team_id == away_team.id
        ).join(
            home_team, db.Fixture.home_team_id == home_team.id
        ).where(
            db.Season.season == season
        ).where(
            away_team.short_name == away
        ).where(
            home_team.short_name == home
        )).one()
    return fixture


def test_fixture_import(import_fixtures):
    import_fixtures.run()

    fixtures = db.dal.session.scalars(select(
        db.Fixture)
    ).all()
    assert len(fixtures) == 2

    fixture_1 = fixture_test_query('2023-24', away='MCI', home='BUR')
    assert fixture_1.away_team_difficulty == 2
    assert fixture_1.home_team_difficulty == 5
    assert fixture_1.away_team_score == 3
    assert fixture_1.home_team_score == 0
    assert fixture_1.fpl_id == 1
    assert fixture_1.fpl_code == 2367538
    assert fixture_1.kickoff_time == datetime(2023,8,11,20)
    assert fixture_1.finished == True
    assert fixture_1.started == True

    
    fixture_50 = fixture_test_query('2023-24', away='LIV', home='WOL')
    assert fixture_50.away_team_difficulty == 2
    assert fixture_50.home_team_difficulty == 4
    assert fixture_50.away_team_score == None
    assert fixture_50.home_team_score == None
    assert fixture_50.fpl_id == 50
    assert fixture_50.fpl_code == 2367587
    assert fixture_50.kickoff_time == datetime(2023,9,16,12,30)
    assert fixture_50.finished == False
    assert fixture_50.started == False
    
    

@pytest.fixture
def import_player_fixtures(import_fixtures):
    orchestrator = import_fixtures
    orchestrator.add_task(player_fixtures, predecessors = {player_seasons, fixtures})
    return orchestrator


def player_fixture_test_query(season, second_name, opp):
    opposition = aliased(db.Team, name='opposition')
    fixture, = db.dal.session.execute(
        select(
            db.PlayerFixture
        ).join(
            db.Fixture, db.PlayerFixture.fixture_id == db.Fixture.id
        ).join(
            db.Season, db.Season.id == db.Fixture.season_id
        ).join(
            opposition, db.PlayerFixture.opposition_id == opposition.id
        ).join(
            db.Player, db.Player.id == db.PlayerFixture.player_id
        ).where(
            db.Season.season == season
        ).where(
            db.Player.second_name == second_name
        ).where(
            opposition.short_name == opp
        )
        ).one()
    return fixture


def test_player_fixture_import(import_player_fixtures):
    import_player_fixtures.run()

    player_fixtures = db.dal.session.scalars(select(
        db.PlayerFixture)
    ).all()
    assert len(player_fixtures) == 2

    fixture_1 = player_fixture_test_query(season='2023-24', second_name='Ramses Becker', opp='WOL')
    assert fixture_1.is_home == False


@pytest.fixture
def import_player_performances(import_player_fixtures):
    orchestrator = import_player_fixtures
    orchestrator.add_task(player_performances, predecessors = {player_seasons, fixtures})
    return orchestrator


def player_performances_test_query(season, second_name, opp):
    opposition = aliased(db.Team, name='opposition')
    performance, = db.dal.session.execute(
        select(
            db.PlayerPerformance
        ).join(
            db.Fixture, db.PlayerPerformance.fixture_id == db.Fixture.id
        ).join(
            db.Season, db.Season.id == db.Fixture.season_id
        ).join(
            opposition, db.PlayerPerformance.opposition_id == opposition.id
        ).join(
            db.Player, db.Player.id == db.PlayerPerformance.player_id
        ).where(
            db.Season.season == season
        ).where(
            db.Player.second_name == second_name
        ).where(
            opposition.short_name == opp
        )
        ).one()
    return performance


def test_player_fixture_import(import_player_performances):
    import_player_performances.run()

    player_performances = db.dal.session.scalars(select(
        db.PlayerPerformance)
    ).all()
    assert len(player_performances) == 2

    performance_1 = player_performances_test_query(season='2023-24', second_name='Álvarez', opp='BUR')
    assert performance_1.was_home == False
    assert performance_1.minutes_played == 90
    assert performance_1.penalties_missed == 0
    assert performance_1.penalties_saved == 0
    assert performance_1.player_value == 65
    assert performance_1.red_cards == 0
    assert performance_1.yellow_cards == 0
    assert performance_1.selected == 396614
    assert performance_1.total_points == 5
    assert performance_1.goals_scored == 0
    assert performance_1.goals_conceded == 0
    assert performance_1.clean_sheet == True
    assert performance_1.bonus == 0
    assert performance_1.assists == 1

    