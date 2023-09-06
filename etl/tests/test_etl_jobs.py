import pytest

from etl.tests.db_fixtures import insert_seasons, database
from etl.tests.utils import PathsForTests


from etl.update.update_pipeline import PipelineOrchestrator, DataImportPipeline
from pathlib import Path


from etl.seed_db.data_files import GameWeekTransformer
from etl.jobs.extractors.extractors import DataTableExtractor
from etl.update.loaders import DBLoader

import database.tables as tbl
from database.data_access_layer import dal

from sqlalchemy import select

def test_gameweek_etl(insert_seasons):
    gameweek = DataImportPipeline(
        DataTableExtractor(['2021-22','2022-23'], Path('gws', 'merged_gw.csv'), pathlib=PathsForTests),
        GameWeekTransformer(),
        DBLoader(tbl.Gameweek)
    )
    gameweek.run()

    gameweeks = dal.session.execute(select(tbl.Gameweek)).all()
    assert len(gameweeks) == 3

