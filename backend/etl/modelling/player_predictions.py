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



def get_data():
    """
    Get the data we are going to need from the database.
    """
    dal.connect()
    dal.session = dal.Session()
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
    dal.session.close()
    return pd.DataFrame(all_performances)

COLUMNS_TO_AGGREGATE = [
            'assists',
            'goals_scored',
            'goals_conceded',
            'clean_sheet',
            'bonus',
            'yellow_cards',
            'saves',
            'bps',
            'threat',
            'influence',
            'creativity'
        ]



def calculate_rolling_means(data, window_size):
        """
        Calculate rolling means for a list of columns specified by: COLUMNS_TO_AGGREGATE
        Calculate for specified window

        e.g. calculate average number of goals over previous 10 games prior to the performance.
        """
        grouped_data = (data
            .groupby(level ='player_id')
            )
        rolling = (grouped_data[COLUMNS_TO_AGGREGATE]
                .rolling(window = window_size + 1, closed = 'left'))
        rolling_mean =  rolling.mean().droplevel(0)
        assert len(rolling_mean) == len(data.index) 
        data = data.join(rolling_mean, rsuffix = f'_{window_size}_game_mean')
        return data


def clean_data(data):
    """
    Clean data:
    - Limit to just games where player played more than 60 minutes.
    - Calculate features based on rolling means for different window sizes.
    - Remove performances where we don't have at least 5 games' worth of prior data - we won't try to predict these.
    """
    data = pd.DataFrame(data)
    data = data[data['minutes_played'] > 60]

    assert data['performance_id'].is_unique

    data = data.set_index(['player_id', 'kickoff_time']).sort_index(inplace=False)

    data = calculate_rolling_means(data, 5)
    data = calculate_rolling_means(data, 10)
    data = calculate_rolling_means(data, 18)

    data = data[data['goals_scored_5_game_mean'].notna()]

    return data

def split_training_and_test(data):
    ## Break into training and test data:
    training_data = data[data['season_start_year'] <= 2021]
    test_data = data[data['season_start_year'] == 2022]
    return training_data, test_data


def generate_data():
    data = get_data()
    data_with_features = clean_data(data)
    training_data, test_data = split_training_and_test(data_with_features)
    training_data.to_csv('training_data.csv')
    test_data.to_csv('test_data.csv')




if __name__ == '__main__':
    generate_data()


