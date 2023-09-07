from sqlalchemy.dialects.postgresql import insert
from database.data_access_layer import dal
from sqlalchemy.orm import  DeclarativeBase
from etl.jobs.loaders.base_loader import Loader
from etl.utils.logging import log


class DBLoader(Loader):
    def __init__(self, table: DeclarativeBase):
        self.table = table
    
    def run(self, data):
        log(f'Loading to table: {self.table}')
        if isinstance(data, dict):
            self.load_single(data)
        elif isinstance(data, list):
            self.load_multiple(data)
    
    def load_multiple(self, data):
        for record in data:
            self.load_single(record)

    def load_single(self, data_dict):
        insert_stmt = insert(
                self.table
            ).values(
                data_dict
            ).on_conflict_do_update(
                constraint= f'{self.table.__tablename__}__prevent_duplicate_import',
                set_=data_dict
            )
        dal.session.execute(insert_stmt)
        
        
