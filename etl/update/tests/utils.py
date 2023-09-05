from etl.update.utils.paths import ProjectPaths

class PathsForTests: 
    @classmethod
    def get_season_data_directory(cls, season):
        return ProjectPaths.project_directory.joinpath('etl', 'test_data', season)