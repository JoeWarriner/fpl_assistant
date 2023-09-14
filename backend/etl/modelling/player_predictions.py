### STEPS
## Prepare raw data - all player performances, split by season
## Calculate rolling averages
## Run a random forest model!
## Integrate back into pipeline

## Get access to imports
import os
import sys
from pathlib import Path
import numpy as np
from datetime import datetime
from typing import Callable, Union
from dataclasses import dataclass
backend_path = Path(__file__).parents[2]
sys.path.append(os.getcwd())


from database.data_access_layer import dal
import database.tables as tbl

from sqlalchemy import select
from sklearn.ensemble  import RandomForestClassifier
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error
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
            tbl.PlayerPerformance.expected_assists,
            tbl.PlayerPerformance.expected_goals,
            tbl.PlayerPerformance.expected_goals_conceded,

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
            'creativity',
            'expected_assists',
            'expected_goals',
            'expected_goals_conceded'
        ]

WINDOW_SIZES = [3, 5,7, 10]



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
    training_data = data[data['kickoff_time'] <= datetime(2023,4,15)]
    test_data = data[data['kickoff_time'] > datetime(2023,4,15)]
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


    # print(feature_columns)
    feature_set = full_dataset[feature_columns]

    position_dummies = pd.get_dummies(full_dataset['position'])
    feature_set = feature_set.join(position_dummies)

    return feature_set




    


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


COLUMN_POINTS_MAP = {
        'assists': get_expected_points_from_assists,
        'goals_scored': get_expected_points_from_goals,
        'goals_conceded': get_points_from_goals_conceded,
        'clean_sheet': get_expected_points_from_clean_sheets,
        'bonus': get_bonus_points,
        'yellow_cards': get_points_from_yellows,
        'saves': get_expected_points_from_saves
}


    

def calculate_mae(target: pd.Series, prediction: pd.Series):
    assert len(target) == len(prediction)
    return np.sum(np.abs(target - prediction)) / len(target)

def calculate_mse(target: pd.Series, prediction: pd.Series):

    return np.sum((target - prediction) * (target - prediction)) / len(target)
    


def fit_underlying_model(
        classifier: RandomForestClassifier, 
        previous_performances: pd.DataFrame, 
        feature_cols: list[str], 
        col_to_predict: str):

    feature_set = previous_performances[feature_cols]
    dependent_var = previous_performances[col_to_predict]
    classifier.fit(feature_set, dependent_var)
    return classifier

def process_future_fixtures(future_fixtures, previous_performances):
    pass



class IndividualOutcomePredictor:

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
        ## Get feature set
        feature_set = self.generate_featureset(self.future_fixtures)
        predicted_probs = self.classifier.predict_proba(feature_set)
        return predicted_probs

    def convert_predicted_probs_to_dataframe(self, predicted_probs):
        # Add results to dataframe
        column_names = [f'prob_{self.col_to_predict}_{i}' for i in range(len(predicted_probs[0]))]
        probs_df = pd.DataFrame(
            columns= column_names,
            data = predicted_probs
        )
        return probs_df
    

    def calculate_expected_values(self, predicted_probs):
        ## Calculate expected value from probabilites:
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




class EventsPredictor:

    individual_predictors: list[IndividualOutcomePredictor]
    previous_performances: pd.DataFrame
    future_fixtures: pd.DataFrame
    generate_featureset: Callable[[pd.DataFrame], pd.DataFrame]
    index_col: str

    def __init__(self, previous_performances: pd.DataFrame, future_fixtures: pd.DataFrame):
        self.classifier = RandomForestClassifier
        self.classifier_kwargs = {'n_estimators': 100}
        self.previous_performances = previous_performances
        self.future_fixtures = future_fixtures
        self.generate_featureset = get_feature_set
        self.index_col = 'performance_id'
        

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
        output = self.future_fixtures.join(individual_predictions).reset_index(names='performance_id')
        return output

    def calculate_final_expected_points(self, predictions: pd.DataFrame, expected_points_cols: list[str]) -> pd.DataFrame:
        first_col, *remaining_cols = expected_points_cols
        predictions['total_expected_points'] = predictions[first_col]
        for col in remaining_cols:
            predictions['total_expected_points'] = predictions['total_expected_points'] + predictions[col]
        return predictions
    
    def convert_to_output_format(self,predictions: pd.DataFrame):
        output_dict = {}
        for _, row in  predictions.iterrows(): 
            output_dict[row[self.index_col]] = row['total_expected_points']
        return output_dict
    

    def run(self):

        individual_predictors = self.generate_individual_predictors()
        individual_prediction_data, expected_points_cols = self.run_individual_predictions(individual_predictors)
        combined_prediction_data = self.combine_individual_predictions(individual_prediction_data)
        self.predictions = self.calculate_final_expected_points(combined_prediction_data, expected_points_cols)
        output = self.convert_to_output_format(self.predictions)
        return output



        
    # def run(self):
    #     generate_data()

    #     ### Save CSVs for visibility
    #     test_data = pd.read_csv('test_data.csv')
    #     test_feature_set = get_feature_set(test_data)
    #     training_data = 
    #     training_feature_set = get_feature_set(training_data)
    







if __name__ == "__main__":
    generate_data()
    test_data = pd.read_csv('test_data.csv')
    training_data = pd.read_csv('training_data.csv')
    predictor = EventsPredictor(previous_performances=training_data, future_fixtures=test_data)
    _ = predictor.run()
    model_output = predictor.predictions





    # for column in list(COLUMN_POINTS_MAP.keys()):

    #     print(f'Starting prediction for: {column}')
    #     classifier = RandomForestClassifier(n_estimators=200)

    #     classifier.fit(training_feature_set, training_data[column])

    #     print(f'Model fit complete, evaluating performance')
    #     score = classifier.score(test_feature_set, test_data[column])
    #     print(f'Mean accuracy is: {score}')
    #     print(classifier.feature_importances_)
    #     print(f'Processing predictions:')
        
        
        
        

    #     expected_values = []
    #     for row in predicted_probs:
    #         expected_value = 0
    #         for i, prob in enumerate(row):
    #             expected_value += i * prob
    #         expected_values.append(expected_value)
        
        
        ## Add pprobabilities to df:
        
        


    # expected_points_list = []
    # for _, row in test_data.iterrows():
    #     expected_points = 0
    #     for column, points_func in COLUMN_POINTS_MAP.items():
    #         expected_points += points_func(row[f'expected_{column}'], row['position'])
    #     expected_points_list.append(expected_points)
    # test_data['expected_points'] = expected_points_list

    ## How did we do?
    
    model_mae = calculate_mae(model_output['total_points'], model_output['total_expected_points'])
    model_mse = calculate_mse(model_output['total_points'], model_output['total_expected_points'])
    
    print(f"MODEL MAE (sklearn): {mean_absolute_error(model_output['total_points'], model_output['total_expected_points'])}")
    print(f"MODEL MSE (sklearn): {mean_squared_error(model_output['total_points'], model_output['total_expected_points'])}")
    print(f"MODEL Median AE (sklearn): {median_absolute_error(model_output['total_points'], model_output['total_expected_points'])}")


    rolling_mean_mae = calculate_mae(model_output['total_points'], model_output['total_points_10_game_mean'])
    rolling_mean_mse = calculate_mse(model_output['total_points'], model_output['total_points_10_game_mean'])

    print(f'ROLLING MEAN MAE: {rolling_mean_mae}')
    print(f'ROLLING MEAN MSE: {rolling_mean_mse}')
    print(f"ROLLING MEAN Median AE (sklearn): {median_absolute_error(model_output['total_points'], model_output['total_points_10_game_mean'])}")

    
    actual_best_performances = model_output.sort_values(by = 'total_points')
    predicted_best_performances = model_output.sort_values(by = 'total_expected_points')
    mean_predicted_best_performances = model_output.sort_values(by = 'total_points_10_game_mean')

    actual_best_performances = actual_best_performances.reset_index(drop = True).reset_index(names='ordering').set_index('performance_id')[['ordering', 'total_points']]
    predicted_best_performances = predicted_best_performances.reset_index(drop = True).reset_index(names='ordering').set_index('performance_id')[['ordering', 'total_expected_points']]
    mean_predicted_best_performances = mean_predicted_best_performances.reset_index(drop = True).reset_index(names='ordering').set_index('performance_id')[['ordering', 'total_points_10_game_mean']]
    
    actual_best_performances = actual_best_performances.join(predicted_best_performances, rsuffix='_predicted')
    actual_best_performances = actual_best_performances.join(mean_predicted_best_performances, rsuffix='_baseline')

    print(f'ORDERING MAE PREDICTION: {mean_absolute_error(actual_best_performances["ordering"], actual_best_performances["ordering_predicted"])}')
    print(f'ORDERING median error PREDICTION: {median_absolute_error(actual_best_performances["ordering"], actual_best_performances["ordering_predicted"])}')
    print(f'ORDERING MAE BASLINE: {mean_absolute_error(actual_best_performances["ordering"], actual_best_performances["ordering_baseline"])}')
    print(f'ORDERING median error BASLINE: {median_absolute_error(actual_best_performances["ordering"], actual_best_performances["ordering_baseline"])}')

    print(f'ORDERING MAE PREDICTION (TOP 100 ): {mean_absolute_error(actual_best_performances["ordering"].tail(100), actual_best_performances["ordering_predicted"].tail(100))}')
    print(f'ORDERING MAE BASLINE (TOP 100): {mean_absolute_error(actual_best_performances["ordering"].tail(100), actual_best_performances["ordering_baseline"].tail(100))}')

    model_output.to_csv('temp_test_data_with_probablities.csv')       




            

        


    





