CREATE TABLE player_fixtures (
	id serial PRIMARY KEY,
    fixture integer,
    player integer,
    team integer,
	current_value integer,
	minutes_played integer,
	penalties_missed integer,
	penalties_saved integer,
	red_cards integer,
	yellow_cards integer,
	selected integer,
	total_points integer,
	goals_scored integer,
	goals_conceded integer,
	clean_sheet boolean,
	bonus integer,
	assists integer,
    FOREIGN KEY (fixture)
        REFERENCES fixtures (id),
    FOREIGN KEY (player)
        REFERENCES players (id),
    FOREIGN KEY (team)
        REFERENCES teams (id)
)