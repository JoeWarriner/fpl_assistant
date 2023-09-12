import modules.team_selector.optimiser as optimiser
from database.test_utils import populated_database_with_predictions

import pandas as pd



def test_get_player_data(populated_database_with_predictions):
    output = optimiser.get_player_data()
    expected_output = pd.DataFrame(
        columns = ['id', 'current_value', 'predicted_score', 'position_id','team_id'],
        data = [[
            2, 55, 2.7, 1, 2
        ]]
    )
    pd.testing.assert_frame_equal(output, expected_output)

