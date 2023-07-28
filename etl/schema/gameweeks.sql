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