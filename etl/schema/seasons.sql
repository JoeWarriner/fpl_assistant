CREATE TABLE seasons (
	id serial PRIMARY KEY,
	is_current boolean,
	start_year integer UNIQUE,
	season varchar(35)
)