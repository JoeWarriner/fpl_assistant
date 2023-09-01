
CREATE TABLE fixtures (
	id serial PRIMARY KEY,
	gameweek integer,
	away_team integer,
	home_team integer,
	away_team_difficulty integer,
	home_team_difficulty integer,
	season integer,
	FOREIGN KEY (gameweek)
		REFERENCES gameweeks (id),
	FOREIGN KEY (away_team)
		REFERENCES teams (id),
	FOREIGN KEY (home_team)
		REFERENCES teams (id),
	FOREIGN KEY (season)
		REFERENCES seasons (id),
	away_team_score integer,
	home_team_score integer,
	fpl_id integer,
	kickoff_time timestamp,
	finished boolean,
	started boolean
)