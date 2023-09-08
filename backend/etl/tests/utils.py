from etl.utils.paths import ProjectPaths
from etl.utils.file_handlers import ProjectFiles

class PathsForTests(ProjectPaths): 
    
    @classmethod
    @property
    def files_directory(cls):
        return cls.project_directory.joinpath('etl', 'tests', 'test_files')
    
    @classmethod
    @property
    def latest_daily_data_dir(cls):
        return cls.files_directory.joinpath('api_data', '2023-09-05')

class ProjectFilesforTests(ProjectFiles):
    pathlib = PathsForTests