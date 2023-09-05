from etl.update.utils.paths import ProjectPaths
import json
import os

class ProjectFiles:
    pathlib = ProjectPaths

    @classmethod
    def summary_api_json(cls):
        with open(cls.pathlib.latest_main_data) as file:
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
    def fixtures_json(cls):
        with open(cls.pathlib.latest_fixture_data) as file:
            return json.loads(file.read())

    @classmethod
    def get_player_detail_json(cls, fpl_id: int, name: str):
        with open(cls.pathlib.get_latest_player_data_path(fpl_id, name)) as file:
            player_data = json.loads(file.read())
        return player_data
    

    @classmethod
    def get_all_player_fixtures(cls):
        all_player_fixtures = []
        for fpl_id, path in cls.pathlib.get_all_player_data_paths():
            with open(path) as file:
                player_data = json.loads(file.read())
                for fixture in player_data['fixtures']:
                    fixture['element'] = fpl_id
                    all_player_fixtures.append(fixture)
        return all_player_fixtures


                
