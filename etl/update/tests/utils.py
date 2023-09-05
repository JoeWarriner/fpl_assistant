from etl.update.utils.paths import ProjectPaths

class PathsForTests(ProjectPaths): 
    
    @classmethod
    @property
    def files_directory(cls):
        return cls.project_directory.joinpath('etl', 'test_files')
    
    
    
