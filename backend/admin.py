from database.data_access_layer import dal
from etl.jobs.base_job import Job
from etl.imports.initial_import import InitialImport
from etl.imports.regular_import import RegularImport

from argparse import ArgumentParser

command_functions = {
    'update' : RegularImport,
    'init': InitialImport
}

parser = ArgumentParser()
parser.add_argument('command')
args = parser.parse_args()
task = command_functions.get(args.command)()
dal.connect()
if args.command == 'init':
    dal.reset_tables()
dal.session = dal.Session()
try:
    task.run()
except Exception as e:
    dal.session.rollback()
    raise Exception from e
else:
    dal.session.commit()
finally:
    dal.session.close()


