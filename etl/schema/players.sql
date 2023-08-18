CREATE TABLE players (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	first_name varchar(35),
	second_name varchar(35)
)