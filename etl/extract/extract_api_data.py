import requests
from pathlib import Path
from datetime import datetime, date
import shutil
import json

FILE_ROOT = Path(__file__).parents[1].joinpath('files')

OVERVIEW_ENDPOINTS = {
    'main': 'https://fantasy.premierleague.com/api/bootstrap-static/',
    'fixtures': 'https://fantasy.premierleague.com/api/fixtures/'
}
PLAYER_DETAIL_ENDPOINT_ROOT = 'https://fantasy.premierleague.com/api/element-summary/'




def get_daily_directory():
    today_date = date.today()
    today_dir = FILE_ROOT.joinpath('api_data', str(today_date))
    try:
        today_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        shutil.rmtree(today_dir)
        today_dir.mkdir(parents=True, exist_ok=False)
    player_dir = today_dir.joinpath('player_details')
    player_dir.mkdir()
    return today_dir


def extract_overview_endpoints(directory: Path):
    for name, endpoint in OVERVIEW_ENDPOINTS.items():
        output = requests.get(endpoint).json()
        filepath = today_dir.joinpath(f'{name}.json')
        with open(filepath, 'w') as file:
            file.write(json.dumps(output))
        

def extract_player_endpoints(directory: Path):
    with open(directory.joinpath('main.json')) as file:
        api_dict = json.loads(file.read())
    players_dict = api_dict['elements']
    for player in players_dict:
        player_endpoint = f'{PLAYER_DETAIL_ENDPOINT_ROOT}{player["id"]}/'
        player_data = requests.get(player_endpoint).json()
        filepath = directory.joinpath('player_details', f'{player["id"]}_{player["second_name"]}.json')
        with open(filepath, 'w') as file:
            file.write(json.dumps(player_data))


if __name__ == "__main__":
    today_dir = get_daily_directory()
    extract_overview_endpoints(directory = today_dir)
    extract_player_endpoints(directory= today_dir)

