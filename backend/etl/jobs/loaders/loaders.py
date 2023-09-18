from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import update

from database.data_access_layer import DataAccessLayer
import database.tables as tbl

from sqlalchemy.orm import  DeclarativeBase
from etl.jobs.loaders.base_loader import Loader
from etl.utils.logging import log

dal = DataAccessLayer()


class DBLoader(Loader):
    def __init__(self, table: DeclarativeBase):
        self.table = table
    
    def transaction_run(self, data):
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
        


class UpdatePredictions(Loader):

    def transaction_run(self, data: dict[int, float]):
        for player_fixture_id, predicted_score in data.items():
            dal.execute_transaction(
                update(
                    tbl.PlayerFixture
                )
                .where(
                    tbl.PlayerFixture.id == player_fixture_id
                ).values(
                    predicted_score = predicted_score
                )
            )