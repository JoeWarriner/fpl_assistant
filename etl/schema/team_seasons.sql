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