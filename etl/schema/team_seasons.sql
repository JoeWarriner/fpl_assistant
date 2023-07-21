CREATE TABLE team_seasons (
	id serial PRIMARY KEY,
	team_fpl_id integer,
	team integer, 
	season integer, 
    FOREIGN KEY (team)
        REFERENCES teams (id),
    FOREIGN KEY (season)
        REFERENCES seasons (id)
)