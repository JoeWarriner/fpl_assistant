CREATE TABLE playerseasons (
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
