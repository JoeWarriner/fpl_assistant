from etl.update.utils.paths import ProjectPaths
import json

class ProjectFiles:

    @classmethod
    def summary_api_json(cls):
        with open(ProjectPaths.latest_main_data) as file:
            all_data = json.loads(file.read())
        return all_data

    @classmethod
    def player_overview_json(cls):
        all_data = cls.summary_api_json()
        return all_data['elements']
    
    @classmethod
    def teams_json(cls):
        all_data = cls.summary_api_json()
        return all_data['teams']

    @classmethod
    def positions_json(cls):
        return cls.summary_api_json()['element_types']

    @classmethod
    def gameweeks_json(cls):
        all_data = cls.summary_api_json()
        return all_data['events']

    @classmethod
    def get_player_detail_json(cls, fpl_id: int, name: str):
        with open(ProjectPaths.get_latest_player_data_path(fpl_id, name)) as file:
            player_data = json.loads(file.read())
        return player_data