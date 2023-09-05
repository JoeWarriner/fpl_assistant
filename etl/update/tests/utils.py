from etl.update.utils.paths import ProjectPaths
from etl.update.utils.file_handlers import ProjectFiles

class PathsForTests(ProjectPaths): 
    
    @classmethod
    @property
    def files_directory(cls):
        return cls.project_directory.joinpath('etl', 'test_files')
    
    @classmethod
    @property
    def latest_daily_data_dir(cls):
        return cls.files_directory.joinpath('api_data', '2023-09-05')
    
class ProjectFilesForTests(ProjectFiles):
    pathlib = PathsForTests