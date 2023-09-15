from etl.jobs.base_job import Job
from database.data_access_layer import dal
from abc import ABC, abstractmethod

class Loader(Job, ABC):
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

        