from etl.jobs.extractors.data_table_extractor import Extractor
from etl.jobs.transformers.base_transformer import Transformer
from etl.jobs.loaders.base_loader import Loader

from etl.jobs.base_job import Job

class DataImportPipeline(Job):
    def __init__(
        self,
        extractor: Extractor,
        transformer: type[Transformer],
        loader: Loader,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):

        data = self.extractor.run()
        
        if self.transformer:
            data = self.transformer.run(data) 
        for record in data:
            self.loader.run(record)
    
    def __repr__(self) -> str:
        return f'Import job for table: {self.loader.table}.'
