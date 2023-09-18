import os
import sys
import numpy as np
from datetime import datetime
from typing import Callable
from dataclasses import dataclass

## Get access to imports (assumes running from backend directory)
sys.path.append(os.getcwd())


from database.data_access_layer import DataAccessLayer
from etl.modelling.basic_model import ModellingJob
import database.tables as tbl
from sqlalchemy import select
from sklearn.ensemble  import RandomForestClassifier
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error, r2_score
import pandas as pd

"""
Code for running and testing/evaluating the random forest prediction model.
"""

"""
Data preparation code
"""

def get_historic_performance_data():
    """
    Get the data we are going to need from the database.
    """

    all_performances = dal.execute_transaction(
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
            tbl.PlayerPerformance.expected_assists,
            tbl.PlayerPerformance.expected_goals,
            tbl.PlayerPerformance.expected_goals_conceded,
            tbl.PlayerPerformance.player_value,

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
            'creativity',
            'expected_assists',
            'expected_goals',
            'expected_goals_conceded'
        ]

WINDOW_SIZES = [4,7,10]



def calculate_rolling_means(data, window_size):
        """
        Calculate rolling means for a list of columns specified by: COLUMNS_TO_AGGREGATE
        Calculate for specified window

        e.g. calculate average number of goals over previous 10 games prior to the performance.
        """
        grouped_data = (data
            .groupby(level ='player_id')
            )
        columns = COLUMNS_TO_AGGREGATE + ['total_points']
        rolling = (grouped_data[columns]
                .rolling(window = window_size + 1, closed = 'left'))
        rolling_mean =  rolling.mean().droplevel(0)
        assert len(rolling_mean) == len(data.index) 
        data = data.join(rolling_mean, rsuffix = f'_{window_size}_game_mean')
        return data

def clean_historic_data(data):
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

def split_training_and_test(data: pd.DataFrame):
    """
    Split data into training and test sets, usign random partition of full dataset.
    Only used for model testing and evaluation.
    """
    shuffled_data = data.sample(frac=1)
    test_data, *training_data = np.array_split(shuffled_data, 4)
    training_data = pd.concat(training_data).reset_index()
    test_data = pd.DataFrame(test_data).reset_index()


    return training_data, test_data


def generate_data_for_testing():
    """
    Data preparation for model testing and evaluation - not used in production.
    """
    data = get_historic_performance_data()
    data_with_features = clean_historic_data(data)
    training_data, test_data = split_training_and_test(data_with_features)
    return training_data,test_data

def get_future_fixtures():
    """
    Get data that will provide the features for prediction in production.
    (i.e. future player fixtures)
    """
    future_fixtures = dal.execute_transaction(select(
        tbl.PlayerFixture.id.label('fixture_id'),
        tbl.PlayerFixture.player_id,
        tbl.PlayerFixture.difficulty,
        tbl.Position.short_name.label('position')
    ).select_from(
        tbl.PlayerFixture
    ).join(
        tbl.Fixture, tbl.PlayerFixture.fixture_id == tbl.Fixture.id
    ).join(
        tbl.PlayerSeason,
        (tbl.PlayerFixture.player_id == tbl.PlayerSeason.player_id) &
        (tbl.PlayerSeason.season_id == tbl.Fixture.season_id)
    ).join(
        tbl.Position,
        tbl.Position.id == tbl.PlayerSeason.position_id
    ).where(
        tbl.Fixture.kickoff_time > datetime.now()
    ))
    return pd.DataFrame(future_fixtures)

def generate_data_for_production():
    """
    Generate data for modelling in production
    """
    # Training data - historic player performances
    data = get_historic_performance_data()
    training_data = clean_historic_data(data)

    # First find the players who have sufficient data to predict
    # (i..e at least 10 previous games worth of data)   
    data = data[data['minutes_played'] > 60]
    data = data.sort_values('kickoff_time')
    recent_performances = data.groupby('player_id').tail(10)
    n_performances = recent_performances.groupby('player_id').size()
    players_with_10_performances = n_performances[n_performances == 10].index
    data_to_analyse = recent_performances[recent_performances['player_id'].isin(players_with_10_performances)]

    # Get full set of future fixtures
    fixtures = get_future_fixtures()
    assert fixtures['fixture_id'].is_unique

    ## We used the last 10 historic performances as features for all future fixtures.
    for window_size in WINDOW_SIZES:
        agg_data = data_to_analyse.groupby('player_id').tail(window_size)
        agg_data = agg_data.groupby('player_id')[COLUMNS_TO_AGGREGATE].mean()
        suffix = f'_{window_size}_game_mean'
        agg_data = agg_data.add_suffix(suffix)
        agg_data = agg_data.reset_index() 
        fixtures = fixtures.join(agg_data, on = 'player_id', rsuffix= suffix)
    fixtures = fixtures.dropna().reset_index()
    assert fixtures['fixture_id'].is_unique


    return training_data, fixtures
        

def get_feature_set(full_dataset: pd.DataFrame, additional_cols =[]):
    """
    Extract only the columns used as features in the model
    """
    feature_columns = []
    for col in COLUMNS_TO_AGGREGATE:
         for n in WINDOW_SIZES:
              feature_columns.append(f'{col}_{n}_game_mean')
    feature_columns.extend(
        ['difficulty'],
    )

    feature_columns.extend(additional_cols)

    # print(feature_columns)
    feature_set = full_dataset[feature_columns]

    position_dummies = pd.get_dummies(full_dataset['position'])
    feature_set = feature_set.join(position_dummies)

    return feature_set



"""
Functions to calculate points from expected measure values.
"""

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


class IndividualOutcomePredictor(ModellingJob):
    """ Produce predicted values for an individual measure (e.g. assists)"""

    def __init__(
            self, 
            classifier: RandomForestClassifier, 
            previous_performances: pd.DataFrame, 
            future_fixtures: pd.DataFrame,
            generate_featureset: Callable[[pd.DataFrame], pd.DataFrame],  
            calculate_points: Callable[[float, str], float],
            col_to_predict: str, 
            index_col: str
        ):
        
        self.classifier = classifier
        self.previous_performances = previous_performances
        self.generate_featureset = generate_featureset
        self.future_fixtures = future_fixtures
        self.calculate_points = calculate_points
        self.col_to_predict = col_to_predict
        self.index_col = index_col 
        self.expected_value_col_name = f'expected_num_{self.col_to_predict}'
        self.expected_points_col_name = f'expected_points_from_{self.col_to_predict}'

    def fit_model(self):
        feature_set = self.generate_featureset(self.previous_performances)
        target_col = self.previous_performances[self.col_to_predict]
        self.classifier.fit(feature_set, target_col)

    def predict_probabilities(self) -> list[list[int]]:
        feature_set = self.generate_featureset(self.future_fixtures)
        predicted_probs = self.classifier.predict_proba(feature_set)
        return predicted_probs

    def convert_predicted_probs_to_dataframe(self, predicted_probs):
        column_names = [f'prob_{self.col_to_predict}_{i}' for i in range(len(predicted_probs[0]))]
        probs_df = pd.DataFrame(
            columns= column_names,
            data = predicted_probs
        )
        return probs_df
    
    def calculate_expected_values(self, predicted_probs):
        expected_values =  []
        for row in predicted_probs:
            expected_value = 0
            for i, prob in enumerate(row):
                expected_value += i * prob
            expected_values.append(expected_value)
        return expected_values
    

    def calculated_expected_points(self, expected_values):
        expected_points = [
            self.calculate_points(expected_value, position)
            for expected_value, position 
            in zip(expected_values, self.future_fixtures['position'])
        ]
        return expected_points
    
    def run(self):
        self.fit_model()
        predicted_probs = self.predict_probabilities()
        expected_values = self.calculate_expected_values(predicted_probs)
        expected_points = self.calculated_expected_points(expected_values)

        output_df = self.convert_predicted_probs_to_dataframe(predicted_probs)
        output_df[self.expected_value_col_name] = expected_values
        output_df[self.expected_points_col_name] = expected_points
        output_df[self.index_col] = self.future_fixtures[self.index_col]

        return output_df, self.expected_points_col_name




class RandomForestCompositePredictor(ModellingJob):
    """
    Class to handle overall random forest prediction.
    Manages and aggregate predictions for all measures."""

    individual_predictors: list[IndividualOutcomePredictor]
    previous_performances: pd.DataFrame
    future_fixtures: pd.DataFrame
    generate_featureset: Callable[[pd.DataFrame], pd.DataFrame]
    index_col: str

    def __init__(self, 
                 index_col: str = 'fixture_id'
        ):

        self.classifier = RandomForestClassifier
        self.classifier_kwargs = {'n_estimators': 2000}
        self.get_data = generate_data_for_production
        self.generate_featureset = get_feature_set
        self.index_col = index_col
        

    def generate_individual_predictors(self) -> list[IndividualOutcomePredictor]:
        column_points_map = {
            'assists': get_expected_points_from_assists,
            'goals_scored': get_expected_points_from_goals,
            'goals_conceded': get_points_from_goals_conceded,
            'clean_sheet': get_expected_points_from_clean_sheets,
            'bonus': get_bonus_points,
            'yellow_cards': get_points_from_yellows,
            'saves': get_expected_points_from_saves
        }

        individual_predictors =  [
            IndividualOutcomePredictor(
                classifier=self.classifier(**self.classifier_kwargs),
                previous_performances=self.previous_performances,
                future_fixtures=self.future_fixtures,
                generate_featureset=self.generate_featureset,
                index_col = self.index_col,

                calculate_points= points_calculator,
                col_to_predict= col
                )

                for col, points_calculator
                in column_points_map.items()
        ]   
        return individual_predictors
    
    def run_individual_predictions(self, individual_predictors: list[IndividualOutcomePredictor]) -> tuple[list[pd.DataFrame], list[str]]:
        probabilities_dfs: list[pd.DataFrame] = []
        expected_points_cols: list[str] = []
        for predictor in individual_predictors:
            probs, expected_points_col = predictor.run()
            expected_points_cols.append(expected_points_col)
            probs.set_index(self.index_col, inplace=True)
            probabilities_dfs.append(probs)

        return probabilities_dfs, expected_points_cols
    
    def combine_individual_predictions(self, individual_predictions: list[pd.DataFrame]) -> pd.DataFrame:
        self.future_fixtures.set_index(self.index_col, inplace=True)
        output = self.future_fixtures.join(individual_predictions).reset_index(names=self.index_col)
        return output

    def calculate_final_expected_points(self, predictions: pd.DataFrame, expected_points_cols: list[str]) -> pd.DataFrame:
        cols = expected_points_cols
        predictions['total_expected_points'] = 2
        for col in cols:
            predictions['total_expected_points'] = predictions['total_expected_points'] + predictions[col]
        return predictions
    
    def convert_to_output_format(self,predictions: pd.DataFrame):
        output_dict = {}
        for _, row in  predictions.iterrows(): 
            output_dict[row[self.index_col]] = row['total_expected_points']
        return output_dict
    
    def run(self):
        self.previous_performances, self.future_fixtures = self.get_data()
        individual_predictors = self.generate_individual_predictors()
        individual_prediction_data, expected_points_cols = self.run_individual_predictions(individual_predictors)
        combined_prediction_data = self.combine_individual_predictions(individual_prediction_data)
        self.predictions = self.calculate_final_expected_points(combined_prediction_data, expected_points_cols)
        output = self.convert_to_output_format(self.predictions)
        return output





"""
MODEL TESTING (this code is not used in the application or pipeline.)

"""


if __name__ == '__main__':  
    # Connect to database
    dal = DataAccessLayer()
    dal.connect()
    dal.session = dal.Session()
         
    # Run prediction with historic data split into training/test.
    predictor = RandomForestCompositePredictor(
        index_col = 'performance_id')
    predictor.get_data = generate_data_for_testing
    _ = predictor.run()
    model_output = predictor.predictions

    # # Save to csv so we can inspect:
    model_output.to_csv('temp__model_evaluation_results.csv') 


    # ## How did we do? Basic MAE / MSE / Median absolute error
    print(f"MODEL MAE : {mean_absolute_error(model_output['total_points'], model_output['total_expected_points'])}")
    print(f"MODEL MSE : {mean_squared_error(model_output['total_points'], model_output['total_expected_points'])}")
    print(f"MODEL Median Absolute Error: {median_absolute_error(model_output['total_points'], model_output['total_expected_points'])}")
    print(f"MODEL R2: {r2_score(model_output['total_points'], model_output['total_expected_points'])}")


    #  I also wanted to look at how well the model RANKED performances 
    #  (Ultimately we're more interested in differentating between performances than predicting their value precisely) 

    # Sort by the different measures (model / baseline / actual)
    ranking_cols = (
        ('total_points','actual_ranking'),
        ('total_expected_points', 'model_ranking'),
        ('total_points_10_game_mean', 'baseline_ranking') 
    )

    for data_column, ranking_column in ranking_cols: 
        model_output = (model_output
                        .sort_values(by=data_column)
                        .reset_index(drop = True)
                        .reset_index(names = ranking_column))
        
    ## Print various metrics to console:
    print(f'MODEL MAE (ORDERING): {mean_absolute_error(model_output["actual_ranking"], model_output["model_ranking"])}')
    print(f'MODEL median absolute error (ORDERING): {median_absolute_error(model_output["actual_ranking"], model_output["model_ranking"])}')

    print(f'BASELINE MAE (ORDERING): {mean_absolute_error(model_output["actual_ranking"], model_output["baseline_ranking"])}')
    print(f'BASELINE median absolute error (ORDERING): {median_absolute_error(model_output["actual_ranking"], model_output["baseline_ranking"])}')

    # How about just the highest performers?
    print(f'MODEL MAE (ORDERING - TOP 100 ): {mean_absolute_error(model_output["actual_ranking"].tail(100), model_output["model_ranking"].tail(100))}')
    print(f'BASELINE MAE (ORDERING - TOP 100): {mean_absolute_error(model_output["actual_ranking"].tail(100), model_output["baseline_ranking"].tail(100))}')

  
    # Close database session      
    dal.session.close()

    





