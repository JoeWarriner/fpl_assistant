CREATE TABLE players (
	id serial PRIMARY KEY,
	fpl_id integer,
	first_name varchar(35),
	second_name varchar(35),
	current_team integer,
	current_cost integer
)