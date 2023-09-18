from etl.pipeline_management.serializers import SimpleSerializer
from etl.jobs.extractors.data_table_extractor import Extractor
from etl.jobs.transformers.base_transformer import Transformer
from etl.jobs.loaders.base_loader import Loader

from etl.jobs.base_job import Job

from etl.pipeline_management.base_pipeline import Pipeline


class DataImportPipeline(Pipeline):
    """
    Wrapper to provide a simplified interface to a pipeline object specific to ETL tasks.
    """
    serializer = SimpleSerializer

    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader):
        super().__init__()
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        self.add_job(self.extractor)
        if self.transformer: 
            self.add_job(self.transformer)
        self.add_job(self.loader)
        super().run()
    
    def __repr__(self) -> str:
        return f'Import job for table: {self.loader.table}.'


class ModellingPipeline(Pipeline):
    """
    Wrapper to provide a simplified interface to Pipeline objects for modelling tasks.
    """
    serializer = SimpleSerializer

    def __init__(self, model: Job, loader: Loader):
        super().__init__()
        self.model = model
        self.loader = loader
    
    def run(self):
        self.add_job(self.model)
        self.add_job(self.loader)
        super().run()
    
    def __repr__(self) -> str:
        return f'Modelling with: {self.model}.'
