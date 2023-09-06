
import database.tables as tbl 
import etl.jobs.api as api
import etl.jobs.extractors.api_extractors as extractor
import etl.jobs.transformers.api_transformers as api_transformers
import etl.jobs.loaders.loaders as loaders
from etl.tests.utils import ProjectFilesForTests
from etl_pipeline import ETLPipeline


class PlayersAPIPipeline(ETLPipeline):
    extractor = extractor.APIExtractor(api.Player, ProjectFilesForTests.player_overview_json),
    transformer = api_transformers.APITransformer(adapter=api_transformers.PlayerAdapter),
    loader = loaders.DBLoader(tbl.Player)

class TeamsAPIPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.Team, ProjectFilesForTests.teams_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.TeamAdapter),
        loader = loaders.DBLoader(tbl.Team)


class PositionAPIPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.Position, ProjectFilesForTests.positions_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.PositionAdapter),
        loader = loaders.DBLoader(tbl.Position)


class PlayerSeasonAPIPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.Player, ProjectFilesForTests.player_overview_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerSeason),
        loader = loaders.DBLoader(tbl.PlayerSeason)


class DataImportPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.Team, ProjectFilesForTests.teams_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.TeamSeasonAdapter),
        loader = loaders.DBLoader(tbl.TeamSeason)



class DataImportPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.GameWeek, ProjectFilesForTests.gameweeks_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.GameWeekAdapter),
        loader = loaders.DBLoader(tbl.Gameweek)


class DataImportPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.Fixture, ProjectFilesForTests.fixtures_json),
        transformer= api_transformers.APITransformer(adapter=api_transformers.FixtureAdapter),
        loader = loaders.DBLoader(tbl.Fixture)


class DataImportPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.PlayerFixture, ProjectFilesForTests.get_all_player_fixtures),
        transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerFixtureAdapter),
        loader = loaders.DBLoader(tbl.PlayerFixture)



class DataImportPipeline(ETLPipeline):
        extractor= extractor.APIExtractor(api.PlayerPerformance, ProjectFilesForTests.get_all_player_performances),
        transformer= api_transformers.APITransformer(adapter=api_transformers.PlayerPerformanceAdapter),
        loader = loaders.DBLoader(tbl.PlayerPerformance)



