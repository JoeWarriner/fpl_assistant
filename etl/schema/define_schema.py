import psycopg2


jobs = [
    'seasons',
    'teams',
    'players',
    'positions',
    'gameweeks',
    'fixtures',
    'player_fixtures',
    'player_seasons',
    'team_seasons'
]

for job in jobs:

    with open(f'etl/schema/{job}.sql') as file:
        create_table_command = file.read()

    drop_table_command = f'DROP TABLE IF EXISTS {job} CASCADE;'
        
    with psycopg2.connect("dbname=fantasyfootballassistant user=postgres") as conn:
        try:
            cur = conn.cursor()
            cur.execute(drop_table_command)
            cur.execute(create_table_command)
        except Exception as e:
            print(job)
            print(drop_table_command)
            print(create_table_command)
            raise e
        
        
