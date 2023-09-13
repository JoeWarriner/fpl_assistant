from datetime import date
import database.tables as tbl 
from etl.jobs.base_job import Job
from etl.pipeline_management.etl_pipeline import DataImportPipeline
from etl.pipeline_management.base_pipeline import Pipeline
from etl.jobs.extractors.seasons_extractor import CreateThisSeason
import etl.jobs.extractors.api.api_models as api_models
import etl.jobs.extractors.api_extractors as extractor
import etl.jobs.transformers.api_transformers as api_transformers
import etl.jobs.loaders.loaders as loaders
from etl.modelling.prediction_modelling import ModelPredictions
from etl.jobs.extractors.api.api_download import APIDownloader
from etl.utils.file_handlers import ProjectFiles



class RegularImport(Job):
    expects_input = False
    file_handler = ProjectFiles
    today_date = date.today()


    def __init__(self):
        self.jobs()
        self.pipeline()


    def jobs(self):
        self.api_download = APIDownloader()


        self.seasons = DataImportPipeline(
            extractor= CreateThisSeason(self.today_date),
            transformer=None,
            loader = loaders.DBLoader(tbl.Season)
        )   

        self.players = DataImportPipeline(
            extractor= extractor.APIExtractor(api_models.Player, self.file_handler.player_overview_json),
            transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerAdapter),
            loader = loaders.DBLoader(tbl.Player)
        )

        self.teams = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.Team, self.file_handler.teams_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.TeamAdapter),
                loader = loaders.DBLoader(tbl.Team)
        )


        self.positions = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.Position, self.file_handler.positions_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.PositionAdapter),
                loader = loaders.DBLoader(tbl.Position)
        )

        self.player_seasons = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.Player, self.file_handler.player_overview_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerSeason),
                loader = loaders.DBLoader(tbl.PlayerSeason)
        )

        self.team_seasons = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.Team, self.file_handler.teams_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.TeamSeasonAdapter),
                loader = loaders.DBLoader(tbl.TeamSeason)
        )


        self.gameweeks = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.GameWeek, self.file_handler.gameweeks_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.GameWeekAdapter),
                loader = loaders.DBLoader(tbl.Gameweek)
        )

        self.fixtures = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.Fixture, self.file_handler.fixtures_json),
                transformer= api_transformers.APITransformer(adapter=api_transformers.FixtureAdapter),
                loader = loaders.DBLoader(tbl.Fixture)
        )

        self.player_fixtures = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.PlayerFixture, self.file_handler.get_all_player_fixtures),
                transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerFixtureAdapter),
                loader = loaders.DBLoader(tbl.PlayerFixture)
        )


        self.player_performances = DataImportPipeline(
                extractor= extractor.APIExtractor(api_models.PlayerPerformance, self.file_handler.get_all_player_performances),
                transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerPerformanceAdapter),
                loader = loaders.DBLoader(tbl.PlayerPerformance)
        )

        self.update_player_predictions = ModelPredictions()
    
    
    def pipeline(self):
        pipeline = Pipeline()
        pipeline.add_task(self.api_download)
        pipeline.add_task(self.seasons)
        pipeline.add_task(self.positions, predecessors={self.api_download})
        pipeline.add_task(self.players, predecessors={self.seasons})
        pipeline.add_task(self.teams, predecessors={self.seasons})
        pipeline.add_task(self.team_seasons, predecessors={self.teams, self.seasons})
        pipeline.add_task(self.gameweeks, predecessors={self.seasons})
        pipeline.add_task(self.fixtures, predecessors={self.team_seasons, self.gameweeks})
        pipeline.add_task(self.player_seasons, predecessors={self.team_seasons, self.seasons, self.players, self.positions})
        pipeline.add_task(self.player_fixtures, predecessors={self.player_seasons, self.fixtures})
        pipeline.add_task(self.player_performances, predecessors = {self.player_seasons, self.fixtures})
        pipeline.add_task(self.update_player_predictions, predecessors = {self.player_performances, self.player_fixtures})
        return pipeline

    def run(self):
        self.pipeline().run()

        

