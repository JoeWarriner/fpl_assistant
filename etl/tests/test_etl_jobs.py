import pytest

from etl.tests.db_fixtures import insert_seasons, database
from etl.tests.utils import PathsForTests


from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
from pathlib import Path


from etl.seed_db.data_files import GameWeekTransformer
from etl.update.extracters import DataTableExtracter
from etl.update.loaders import DBLoader

import etl.update.database as db
from sqlalchemy import select

def test_gameweek_etl(insert_seasons):
    gameweek = DataImportPipeline(
        DataTableExtracter(['2021-22','2022-23'], Path('gws', 'merged_gw.csv'), pathlib=PathsForTests),
        GameWeekTransformer(),
        DBLoader(db.Gameweek)
    )
    gameweek.run()

    gameweeks = db.dal.session.execute(select(db.Gameweek)).all()
    assert len(gameweeks) == 3

