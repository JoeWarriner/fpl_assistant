CREATE TABLE positions (
	id serial PRIMARY KEY,
	fpl_id integer UNIQUE,
	pos_name varchar(35),
	short_name varchar(5)
)
