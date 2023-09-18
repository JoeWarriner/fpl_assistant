import requests
from pathlib import Path
from etl.utils.paths import ProjectPaths
import shutil
import json

"""
Code to download the full FPL API output and save to file.
"""

MAIN_ENDPOINT = 'https://fantasy.premierleague.com/api/bootstrap-static/'
FIXTURES_ENDPOINT = 'https://fantasy.premierleague.com/api/fixtures/'
PLAYER_DETAIL_ENDPOINT_ROOT = 'https://fantasy.premierleague.com/api/element-summary/'


def daily_directory_exists():
    return ProjectPaths.latest_daily_data_dir.is_dir()


def get_daily_directory():
    today_dir = ProjectPaths.latest_daily_data_dir
    try:
        today_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        shutil.rmtree(today_dir)
        today_dir.mkdir(parents=True, exist_ok=False)
    player_dir = today_dir.joinpath('player_details')
    player_dir.mkdir()
    return today_dir


def extract_to_file(endpoint_url: str, path: Path):
    output = requests.get(endpoint_url).json()
    with open(path, 'w') as file:
        file.write(json.dumps(output))

        
def extract_detailed_player_data():
    with open(ProjectPaths.latest_main_data) as file:
        player_summary_data = json.loads(file.read())['elements']
    for player in player_summary_data:
        player_endpoint = f'{PLAYER_DETAIL_ENDPOINT_ROOT}{player["id"]}/'
        filepath = ProjectPaths.latest_player_data_dir.joinpath(
            f'{player["id"]}_{player["second_name"]}.json'
        )
        extract_to_file(player_endpoint, filepath)
        

class APIDownloader:
    expects_input = False

    def run(self):
        if not daily_directory_exists():
            today_dir = get_daily_directory()
            extract_to_file(MAIN_ENDPOINT, ProjectPaths.latest_main_data)
            extract_to_file(FIXTURES_ENDPOINT, ProjectPaths.latest_fixture_data)
            extract_detailed_player_data()

