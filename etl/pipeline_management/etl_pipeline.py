from etl.pipeline_management.serializers import SimpleSerializer
from etl.jobs.extractors.data_table_extractor import Extractor
from etl.jobs.transformers.base_transformer import Transformer
from etl.jobs.loaders.base_loader import Loader

from etl.pipeline_management.base_pipeline import Pipeline

class DataImportPipeline(Pipeline):
    task_serializer = SimpleSerializer

    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader ):
        super().__init__()
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        self.add_task(self.extractor)
        if self.transformer: 
            self.add_task(self.transformer)
        self.add_task(self.loader)
        super().run()
    
    def __repr__(self) -> str:
        return f'Import job for table: {self.loader.table}.'




    
