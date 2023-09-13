### STEPS
## Prepare raw data - all player performances, split by season
## Calculate rolling averages
## Run a random forest model!
## Integrate back into pipeline

## Get access to imports
import os
import sys
from pathlib import Path

backend_path = Path(__file__).parents[2]
sys.path.append(os.getcwd())


from database.data_access_layer import dal
import database.tables as tbl

from sqlalchemy import select

import pandas as pd

## Connect dal
dal.connect()
dal.session = dal.Session()


### Get underlying data.
all_performances = dal.execute(
    select(
        
        # Identifier
        tbl.PlayerPerformance.id.label('performance_id'),

        # Overall target measure
        tbl.PlayerPerformance.total_points,

        # Target sub-measures
        tbl.PlayerPerformance.assists,
        tbl.PlayerPerformance.goals_scored,
        tbl.PlayerPerformance.goals_conceded,
        tbl.PlayerPerformance.clean_sheet,
        tbl.PlayerPerformance.bonus,
        tbl.PlayerPerformance.yellow_cards,
        tbl.PlayerPerformance.saves,

        # Underyling data
        tbl.PlayerPerformance.expected_assists,
        tbl.PlayerPerformance.expected_goals,
        tbl.PlayerPerformance.expected_goals_conceded,
        tbl.PlayerPerformance.bps,
        tbl.PlayerPerformance.threat,
        tbl.PlayerPerformance.influence,
        tbl.PlayerPerformance.creativity,

        # Context data:
        tbl.PlayerPerformance.difficulty,
        tbl.PlayerPerformance.minutes_played,
        tbl.Position.id.label('position_id'),
        tbl.Fixture.kickoff_time,
        
        # Human readable identifying data
        tbl.Season.start_year.label('season_start_year'),
        tbl.Player.second_name.label('player_name'),
        tbl.Position.short_name.label('position'),
        tbl.Player.id.label('player_id')
        
        )
        .select_from(tbl.PlayerPerformance)
        .join(tbl.Player, tbl.PlayerPerformance.player_id == tbl.Player.id)
        .join(tbl.Fixture, tbl.Fixture.id == tbl.PlayerPerformance.fixture_id)
        .join(tbl.Season, tbl.Fixture.season_id == tbl.Season.id)
        .join(tbl.PlayerSeason, 
              (tbl.PlayerPerformance.player_id == tbl.PlayerSeason.player_id) &
              (tbl.PlayerSeason.season_id == tbl.Season.id)
            )
        .join(tbl.Position, tbl.Position.id == tbl.PlayerSeason.position_id)
        
    ).all()

data = pd.DataFrame(all_performances)
data = data[data['minutes_played'] > 60]
data.to_csv('modelling_data_temp_1.csv')

assert data['performance_id'].is_unique

data = data.set_index(['player_id', 'kickoff_time']).sort_index(inplace=False)


## Calculate rolling mean from previous 10 performances:

columns_to_aggregate = [
        'assists',
        'goals_scored',
        'goals_conceded',
        'clean_sheet',
        'bonus',
        'yellow_cards',
        'saves',
        'expected_assists',
        'expected_goals',
        'expected_goals_conceded',
        'bps',
        'threat',
        'influence',
        'creativity'
    ]
grouped_data = (data
    .groupby(level ='player_id')
    )
rolling = (grouped_data[columns_to_aggregate]
           .rolling(window = 11, closed = 'left'))
rolling_mean =  rolling.mean().droplevel(0)
assert len(rolling_mean) == len(data.index) 
data = data.join(rolling_mean, rsuffix = '_10_game_mean')
data.to_csv('modelling_data_temp_2.csv')


## Break into training and test data:
training_data = data[data['season_start_year'] <= 2021]
test_data = data[data['season_start_year'] == 2022]

dal.session.close()


