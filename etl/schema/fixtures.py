from sqlalchemy import Table, Column, Integer, String, ForeignKey, DateTime, Boolean


def fixtures_table(metadata):
	fixtures = Table('fixtures', metadata,
			Column('fixture_id', Integer(), primary_key=True),
			Column('gameweek', Integer()),
			Column('away_team', ForeignKey('teams.team_id')),
			Column('home_team', ForeignKey('teams.team_id')),
			Column('season', ForeignKey('season.season_id')),
			Column('away_team_difficulty', Integer()),
			Column('home_team_difficulty', Integer()),
			Column('home_team_score', Integer()),
			Column('fpl_fixture_id', Integer()),
			Column('kickoff_time', DateTime()),
			Column('finished', Boolean()),
			Column('started', Boolean()),
	)


