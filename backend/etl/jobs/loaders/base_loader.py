from etl.jobs.base_job import Job
from database.data_access_layer import DataAccessLayer
from abc import ABC, abstractmethod

dal = DataAccessLayer()

class Loader(Job, ABC):
    """
    Base loader class. 
    Handles transaction commit and rollback for the entire load operation.
    """
    expects_input = True
    
    @abstractmethod
    def transaction_run():
        ...

    def run(self, *args, **kwargs):
        try:
            self.transaction_run(*args, **kwargs)
        except Exception as e:
            dal.session.rollback()
            raise Exception from e
        else:
            dal.session.commit()

        