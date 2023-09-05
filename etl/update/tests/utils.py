from etl.update.utils.paths import ProjectPaths
from etl.update.utils.file_handlers import ProjectFiles

class PathsForTests(ProjectPaths): 
    
    @classmethod
    @property
    def files_directory(cls):
        return cls.project_directory.joinpath('etl', 'test_files')
    
    
    
class ProjectFilesForTests(ProjectFiles):
    pathlib = PathsForTests