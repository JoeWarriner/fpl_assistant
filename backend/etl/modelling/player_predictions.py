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
from sklearn.ensemble  import RandomForestClassifier
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

WINDOW_SIZES = [5, 10]



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

    for n in WINDOW_SIZES:
        data = calculate_rolling_means(data, n)
    

    data = data.dropna()
    data = data.reset_index()
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


def get_feature_set(full_dataset: pd.DataFrame):
    feature_columns = []
    for col in COLUMNS_TO_AGGREGATE:
         for n in WINDOW_SIZES:
              feature_columns.append(f'{col}_{n}_game_mean')
    feature_columns.extend(
        ['difficulty'],
    )

    feature_set = full_dataset[feature_columns]

    position_dummies = pd.get_dummies(full_dataset['position'])
    feature_set = feature_set.join(position_dummies)

    return feature_set


COLUMNS_TO_PREDICT = [
        'assists',
        'goals_scored',
        'goals_conceded',
        'clean_sheet',
        'bonus',
        'yellow_cards',
        'saves'
]


    


def get_expected_points_from_assists(expected_assists, *args, **kwargs):
    return expected_assists * 3

def get_expected_points_from_goals(expected_goals, position):
    if position == 'GKP' or position == 'DEF':
        return expected_goals * 6
    elif position == 'MID':
        return expected_goals * 5
    elif position == 'FWD':
        return expected_goals * 4
    else:
        raise ValueError(f'Position type {position} not recognised')
    

def get_expected_points_from_clean_sheets(expected_clean_sheets, position):
    if position == 'GKP' or position == 'DEF':
        return expected_clean_sheets * 4
    elif position == 'MID':
        return expected_clean_sheets * 1
    elif position == 'FWD':
        return 0
    else:
        raise ValueError(f'Position type {position} not recognised')

def get_expected_points_from_saves(expected_saves, position):
    if position == 'GKP':
        return expected_saves / 3
    elif position in ['MID', 'FWD', 'DEF']:
        return 0
    else:
        raise ValueError(f'Position type {position} not recognised')
    
def get_bonus_points(expected_bonus_points, *args, **kwargs):
    return expected_bonus_points

def get_points_from_yellows(expected_yellows, *args, **kwargs):
    return expected_yellows * -1

def get_points_from_goals_conceded(expected_goals_conceded, position):
    if position in ['GKP', 'DEF']:
        return expected_goals_conceded * -0.5
    elif position in ['MID', 'FWD']:
        return 0
    else:
        raise ValueError(f'Position type {position} not recognised')


    





if __name__ == '__main__':
    generate_data()
    test_data = pd.read_csv('test_data.csv')
    test_feature_set = get_feature_set(test_data)

    training_data = pd.read_csv('training_data.csv')
    training_feature_set = get_feature_set(training_data)



    for column in COLUMNS_TO_PREDICT:

        print(f'Starting prediction for: {column}')
        classifier = RandomForestClassifier(n_estimators=100)
        classifier.fit(training_feature_set, training_data[column])

        print(f'Model fit complete, evaluating performance')
        score = classifier.score(test_feature_set, test_data[column])
        print(f'Mean accuracy is: {score}')

        print(f'Processing predictions:')
        predicted_probs = classifier.predict_proba(training_feature_set)
        columns = [f'prob_{column}_{i}' for i in range(len(predicted_probs[0]))]
        
        probs_df = pd.DataFrame(
            columns = columns,
            data = predicted_probs
        )
        print(probs_df)
        for col in columns:
            print(probs_df[col])
            test_data[col] = probs_df[col]
            print(test_data[col])

    test_data.to_csv('test_data_with_probablities.csv')
            

            

        


    





