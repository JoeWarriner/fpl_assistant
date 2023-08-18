CREATE TABLE teams (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	team_name varchar,
	short_name varchar
)