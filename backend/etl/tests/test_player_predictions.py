import pytest

from etl.modelling.random_forest import RandomForestCompositePredictor, IndividualOutcomePredictor, get_expected_points_from_goals
import pandas as pd

class TestRandomForestCompositePredictor:
    def test_calculate_final_expected_points(self):
        """Test collation of expected points for different measures"""
        test_df = pd.DataFrame(
            columns = ['a', 'b', 'c', 'd'],
            data =[
                [1, 2, 3,4 ],
                [5,6,7,8]
            ]
        )
        test_cols = ['a', 'd']
        predictor = RandomForestCompositePredictor()
        output = predictor.calculate_final_expected_points(test_df, test_cols)

        expected_output = pd.DataFrame(
            columns = ['a', 'b', 'c', 'd', 'total_expected_points'],
            data =[
                [1, 2, 3,4 , 7],
                [5,6,7,8, 15]
            ]
        )

        pd.testing.assert_frame_equal(output,expected_output)



@pytest.fixture
def individual_outcome_predictor():
    return IndividualOutcomePredictor(
        classifier=None,
        previous_performances=None,
        future_fixtures=None,
        generate_featureset=None,
        calculate_points=None,
        col_to_predict='goals_scored',
        index_col='performance_id'
    )

class MockClassifier:
    def __init__(self, probs:list[list[int]]):
        self.probs = probs

    def fit(self, *args, **kwargs):
        pass

    def predict_proba(self, *args, **kwargs):
        return self.probs
    


class TestIndividualOutcomePredictor:
    def test_add_predicted_probs_to_dataframe(
            self,                                
            individual_outcome_predictor: IndividualOutcomePredictor
        ):
        """ Test converting predicted probabilities from array to dataframe."""

        test_probs = [
            [2,2,2],
            [4, 5,6]
        ]
        output = individual_outcome_predictor.convert_predicted_probs_to_dataframe(test_probs)
        expected_output = pd.DataFrame(
            columns = ['prob_goals_scored_0','prob_goals_scored_1','prob_goals_scored_2'],
            data = test_probs
        )
        pd.testing.assert_frame_equal(output,expected_output)

    def test_calculate_expected_values(
            self,                                
            individual_outcome_predictor: IndividualOutcomePredictor
        ):
        """Test calculation of expected measure values from probabilities """

        test_probs = [
            [2,2,2],
            [4, 5,6]
        ]
        output = individual_outcome_predictor.calculate_expected_values(test_probs)
        expected_output = [6, 17]
        assert output == expected_output
    
    def test_calculated_expected_points(
            self,
            individual_outcome_predictor: IndividualOutcomePredictor
            ):
        """Test calculation of expected points from expected values."""

        test_fixtures = pd.DataFrame(
            columns = ['position'], 
            data = [['MID'], ['DEF'], ['FWD'], ['MID']])
        individual_outcome_predictor.future_fixtures = test_fixtures
        individual_outcome_predictor.calculate_points = get_expected_points_from_goals

        test_expected_values = [0, 1, 2, 3]
        
        output = individual_outcome_predictor.calculated_expected_points(
            test_expected_values
            )

        expected_output = [0, 6, 8, 15]
        assert output == expected_output
    
    def test_complete_predictor(self, individual_outcome_predictor: IndividualOutcomePredictor):
        """End to end test of individual measure predition"""
        test_probs = [
            [0.5, 0.3,0.2],
            [1, 0,0]
        ]
        individual_outcome_predictor.generate_featureset = lambda x : x
        individual_outcome_predictor.calculate_points = get_expected_points_from_goals

        individual_outcome_predictor.previous_performances = pd.DataFrame(
            columns = ['goals_scored'], 
            data = [[0]]
        )
        individual_outcome_predictor.future_fixtures = pd.DataFrame(
            columns = ['position', 'performance_id'], 
            data = [['MID', 0], ['DEF', 1]])
        individual_outcome_predictor.classifier = MockClassifier(test_probs)
        output_df, output_str = individual_outcome_predictor.run()

        expected_output_df = pd.DataFrame(
            data = {
                'performance_id': [0, 1], 
                'prob_goals_scored_0': [0.5, 1],
                'prob_goals_scored_1': [0.3, 0],
                'prob_goals_scored_2': [0.2, 0],
                'expected_num_goals_scored': [0.7, 0],
                'expected_points_from_goals_scored' : [3.5, 0],
                }
        )
        pd.testing.assert_frame_equal(output_df, expected_output_df, check_like=True)
        assert output_str == 'expected_points_from_goals_scored'


