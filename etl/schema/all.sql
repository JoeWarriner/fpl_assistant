
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

CREATE TABLE gameweeks (
	id serial PRIMARY KEY,
    deadline_time timestamp,
	finished boolean,
	is_previous boolean,
	is_next boolean,
	is_current boolean,
	gw_number integer,
    season integer,
	FOREIGN KEY (season)
        REFERENCES seasons (id)
)


CREATE TABLE player_fixtures (
	id serial PRIMARY KEY,
    fixture integer,
    player integer,
    team integer,
	opposition integer,
	player_value integer,
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
	was_home boolean,
    FOREIGN KEY (fixture)
        REFERENCES fixtures (id),
    FOREIGN KEY (player)
        REFERENCES players (id),
    FOREIGN KEY (team)
        REFERENCES teams (id)
)

CREATE TABLE player_seasons (
	id serial PRIMARY KEY,
	fpl_id integer, 
	player integer,
	season integer,
	position integer,
    FOREIGN KEY (player)
        REFERENCES players (id),
    FOREIGN KEY (season)
        REFERENCES seasons (id),
    FOREIGN KEY (position)
        REFERENCES positions (id)
)

CREATE TABLE players (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	first_name varchar(35),
	second_name varchar(35)
)

CREATE TABLE positions (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	pos_name varchar(35),
	short_name varchar(5)
)

CREATE TABLE seasons (
	id serial PRIMARY KEY,
	is_current boolean,
	start_year integer UNIQUE,
	season varchar(35)
)

CREATE TABLE team_seasons (
	id serial PRIMARY KEY,
	fpl_id integer,
	team integer, 
	season integer, 
    FOREIGN KEY (team)
        REFERENCES teams (id),
    FOREIGN KEY (season)
        REFERENCES seasons (id)
)

CREATE TABLE teams (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	team_name varchar,
	short_name varchar
)