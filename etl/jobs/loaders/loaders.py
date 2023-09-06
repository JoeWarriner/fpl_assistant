from sqlalchemy.dialects.postgresql import insert
from database.data_access_layer import dal
from sqlalchemy.orm import  DeclarativeBase
from etl.jobs.loaders.base_loader import Loader


class DBLoader(Loader):
    def __init__(self, table: DeclarativeBase):
        self.table = table
    
    def run(self, data_dict): 
        insert_stmt = insert(
                self.table
            ).values(
                data_dict
            ).on_conflict_do_update(
                constraint= f'{self.table.__tablename__}__prevent_duplicate_import',
                set_=data_dict
            )
        dal.session.execute(insert_stmt)
        
        
