SELECT 
    first_fixture.short_name as element_type,
    first_fixture.player_value as now_cost,
    first_fixture.second_name as full_name,
    first_fixture.team as team,
    last_seasons_totals.points as total_points

FROM
    (
        SELECT
            player,
            SUM(total_points) as points
        FROM
            player_fixtures 
            INNER JOIN fixtures on player_fixtures.fixture = fixtures.id
            INNER JOIN seasons on fixtures.season = seasons.id
        WHERE
            seasons.start_year = 2022
        GROUP BY
            player
    ) as last_seasons_totals 
    INNER JOIN
    ( SELECT 
        player_fixtures.player,
        players.second_name,
        player_fixtures.player_value,
        player_fixtures.team,
        positions.short_name
    FROM
        player_fixtures
        INNER JOIN fixtures on player_fixtures.fixture = fixtures.id
        INNER JOIN gameweeks on fixtures.gameweek = gameweeks.id
        INNER JOIN player_seasons on player_fixtures.player = player_seasons.player and player_seasons.season = gameweeks.season
        INNER JOIN positions on positions.id = player_seasons.position
        INNER JOIN players on players.id = player_fixtures.player
    WHERE
        gameweeks.is_next = TRUE
    ) as first_fixture on last_seasons_totals.player = first_fixture.player;
    




    
