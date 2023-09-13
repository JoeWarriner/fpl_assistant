from sqlalchemy import select

import database.tables as tbl


PLAYERS = (
            select(
                tbl.Player.id,
                tbl.Player.first_name,
                tbl.Player.second_name,
                tbl.Player.current_value,
                tbl.PlayerFixture.predicted_score,
                tbl.Position.short_name.label('position'),
                tbl.Team.short_name.label('team')
            )
            .select_from(tbl.Player)
            .join(
                tbl.PlayerFixture, 
                tbl.PlayerFixture.player_id == tbl.Player.id)
            .join(
                tbl.Fixture, 
                tbl.PlayerFixture.fixture_id == tbl.Fixture.id )
            .join(
                tbl.Gameweek, 
                tbl.Gameweek.id == tbl.Fixture.gameweek_id)
            .join(
                tbl.PlayerSeason, 
                (tbl.Player.id == tbl.PlayerSeason.player_id) & 
                (tbl.PlayerSeason.season_id == tbl.Gameweek.season_id)
            )
            .join(
                tbl.Team, 
                tbl.Team.id == tbl.PlayerFixture.team_id)
            .join(
                tbl.Position, 
                tbl.Position.id == tbl.PlayerSeason.position_id)
            .where(
                tbl.Gameweek.is_next == True)
            )