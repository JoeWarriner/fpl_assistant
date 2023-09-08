import pytest
from database.data_access_layer import dal
import database.tables as tbl
from sqlalchemy import insert



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
            tbl.Season.is_current: True
        }
    )
    )


@pytest.fixture
def insert_players(database):
    dal.session.execute(
        insert(tbl.Player).values({
            tbl.Player.id: 1,
            tbl.Player.first_name: "Julián",
            tbl.Player.second_name: "Álvarez",
            tbl.Player.fpl_id: 461358
        }
    ))
    dal.session.execute(
        insert(tbl.Player).values({
            tbl.Player.id: 2,
            tbl.Player.first_name: "Alisson",
            tbl.Player.second_name: "Ramses Becker",
            tbl.Player.fpl_id: 116535
        })
    )

    dal.session.execute(
        insert(tbl.Player).values({
            tbl.Player.id: 3,
            tbl.Player.first_name: "Fabio Henrique",
            tbl.Player.second_name: "Tavares",
            tbl.Player.fpl_id: 116643
        })
    )

@pytest.fixture
def insert_teams(database):
    dal.session.execute(
        insert(tbl.Team).values({
            tbl.Team.id: 1,
            tbl.Team.fpl_id: 43,
            tbl.Team.team_name: "Man City",
            tbl.Team.short_name: 'MCI'
        }
    ))  
    dal.session.execute(
        insert(tbl.Team).values({
            tbl.Team.id: 2,
            tbl.Team.fpl_id: 14,
            tbl.Team.team_name: "Liverpool",
            tbl.Team.short_name: 'LIV'
        }
    )) 


@pytest.fixture
def insert_positions(database):
    dal.session.execute(
        insert(tbl.Position).values({
            tbl.Position.id: 1,
            tbl.Position.fpl_id: 1,
            tbl.Position.pos_name: "Goalkeeper",
            tbl.Position.short_name: 'GKP'
        })
    )
    dal.session.execute(
        insert(tbl.Position).values({
            tbl.Position.id: 2,
            tbl.Position.fpl_id: 2,
            tbl.Position.pos_name: "Defender",
            tbl.Position.short_name: 'DEF'
        })
    )
    dal.session.execute(
        insert(tbl.Position).values({
            tbl.Position.id: 3,
            tbl.Position.fpl_id: 3,
            tbl.Position.pos_name: "Midfielder",
            tbl.Position.short_name: 'MID'
        })
    )
    dal.session.execute(
        insert(tbl.Position).values({
            tbl.Position.id: 4,
            tbl.Position.fpl_id: 4,
            tbl.Position.pos_name: "Forward",
            tbl.Position.short_name: 'FWD'
        })
    )

@pytest.fixture
def insert_player_seasons(insert_players, insert_teams, insert_positions, insert_seasons):
    dal.session.execute(
        insert(tbl.PlayerSeason).values({
            tbl.PlayerSeason.id: 1,
            tbl.PlayerSeason.fpl_id: 343,
            tbl.PlayerSeason.player_id: 1,  # Alvarez
            tbl.PlayerSeason.season_id: 2,  # 2022-23
            tbl.PlayerSeason.position_id: 4 # FWD
        })
    )

    dal.session.execute(
        insert(tbl.PlayerSeason).values({
            tbl.PlayerSeason.id: 2,
            tbl.PlayerSeason.fpl_id: 291,
            tbl.PlayerSeason.player_id: 2,  # Alisson
            tbl.PlayerSeason.season_id: 2,  # 2022-23
            tbl.PlayerSeason.position_id: 1 # GKP 
        })
    )

    dal.session.execute(
        insert(tbl.PlayerSeason).values({
            tbl.PlayerSeason.id: 3,
            tbl.PlayerSeason.fpl_id: 232,
            tbl.PlayerSeason.player_id: 3,  # Fabinho
            tbl.PlayerSeason.season_id: 1,  # 2021-22
            tbl.PlayerSeason.position_id: 3 # MID 
        })
    )


