import database.tables as tbl 
from pathlib import Path
from etl.jobs.base_job import Job
from etl.pipeline_management.task_pipelines import DataImportPipeline
from etl.pipeline_management.base_pipeline import Pipeline
from etl.utils.paths import ProjectPaths
import etl.jobs.extractors.seasons_extractor as seasons_extractor
import etl.jobs.extractors.api.api_models as api_models
from etl.jobs.extractors.api.api_download import APIDownloader
from etl.jobs.extractors.api_extractors import APIExtractor
from etl.jobs.extractors.data_table_extractor import DataTableExtractor
import etl.jobs.transformers.data_table_transformers as tform
from etl.jobs.transformers.api_transformers import APITransformer, PositionAdapter
from etl.jobs.loaders.loaders import DBLoader
from etl.utils.file_handlers import ProjectFiles

"""
Comtainer class to specify jobs/pipelines run in the initial database import.
"""

class InitialImport(Job):
    expects_input = False
    pathlib = ProjectPaths
    seasons_to_import = ['2018-19', '2019-20', '2020-21', '2021-22', '2022-23']

    def __init__(self):
        self.jobs()
        self.pipeline()

    def jobs(self):
        self.seasons = DataImportPipeline(
                extractor=seasons_extractor.CreateSeasonsFromList(self.seasons_to_import),
                transformer=None,
                loader= DBLoader(tbl.Season)
        )

        self.players = DataImportPipeline(
                extractor = DataTableExtractor(self.seasons_to_import, 'players_raw.csv', pathlib = self.pathlib),
                transformer = tform.PlayerTransformer(),
                loader =  DBLoader(tbl.Player)
            )

        self.teams = DataImportPipeline(
            extractor= DataTableExtractor(self.seasons_to_import, 'teams.csv', pathlib = self.pathlib),
            transformer= tform.TeamTransformer(),
            loader = DBLoader(tbl.Team)
        )

        self.team_seasons = DataImportPipeline(
            extractor= DataTableExtractor(self.seasons_to_import, 'players_raw.csv', pathlib = self.pathlib),
            transformer= tform.TeamSeasonTransformer(),
            loader=DBLoader(tbl.TeamSeason)
        )

        self.positions = DataImportPipeline(
            extractor = APIExtractor(api_models.Position, ProjectFiles.positions_json),
            transformer = APITransformer(PositionAdapter),
            loader = DBLoader(tbl.Position)
        )

        self.player_seasons = DataImportPipeline(
            extractor= DataTableExtractor(self.seasons_to_import, 'players_raw.csv', pathlib = self.pathlib),
            transformer=tform.PlayerSeasonTransformer(),
            loader = DBLoader(tbl.PlayerSeason)
        )

        self.gameweeks = DataImportPipeline(
            extractor = DataTableExtractor(self.seasons_to_import,'fixtures.csv', pathlib = self.pathlib),
            transformer= tform.GameWeekTransformer(),
            loader = DBLoader(tbl.Gameweek)
        )

        self.fixtures = DataImportPipeline(
            extractor = DataTableExtractor(self.seasons_to_import, 'fixtures.csv', pathlib = self.pathlib),
            transformer=tform.FixturesTransformer(),
            loader=DBLoader(tbl.Fixture)
        )

        self.player_performances = DataImportPipeline(
            extractor=DataTableExtractor(self.seasons_to_import, Path('gws', 'merged_gw.csv'), pathlib = self.pathlib),
            transformer= tform.PlayerPerformanceTransformer(),
            loader= DBLoader(tbl.PlayerPerformance)
        )

        self.api_download = APIDownloader()
    
    def pipeline(self):
        pipeline = Pipeline()
        pipeline.add_job(self.api_download)
        pipeline.add_job(self.positions, predecessors={self.api_download})
        pipeline.add_job(self.seasons)
        pipeline.add_job(self.players, predecessors={self.seasons})
        pipeline.add_job(self.teams, predecessors={self.seasons})
        pipeline.add_job(self.player_seasons, predecessors={self.seasons, self.players, self.team_seasons, self.positions})
        pipeline.add_job(self.team_seasons, predecessors={self.teams, self.seasons})
        pipeline.add_job(self.gameweeks, predecessors={self.seasons})
        pipeline.add_job(self.fixtures, predecessors={self.team_seasons, self.gameweeks})
        pipeline.add_job(self.player_performances, predecessors = {self.player_seasons, self.fixtures})
        return pipeline

    def run(self):
        self.pipeline().run()