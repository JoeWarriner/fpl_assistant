SELECT
    player
    -- SUM(total_points) as points
FROM
    player_fixtures 
    INNER JOIN fixtures on player_fixtures.fixture = fixtures.id
    INNER JOIN seasons on fixtures.season = seasons.id
WHERE
    seasons.start_year = 2022
-- GROUP BY
--     player



