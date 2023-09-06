from __future__ import annotations
from typing import Protocol
import sqlalchemy
from abc import ABC, abstractmethod
from copy import copy
from sqlalchemy.orm import Session

import etl.jobs.extractors.data_table_extractor as extract
from etl.jobs.extractors.data_table_extractor import Extractor
from etl.jobs.transformers.base_transformer import Transformer
from etl.jobs.loaders.base_loader import Loader
import etl.update.api as api
import etl.jobs.transformers.api_transformers as tform
import etl.jobs.loaders.loaders as load
from etl.utils.file_handlers import ProjectFiles
from database.tables import Gameweek





class Task(ABC):
    @abstractmethod
    def run(): 
        ...


class DataImportPipeline(Task):
    def __init__(
        self,
        extractor: Extractor,
        transformer: type[Transformer],
        loader: Loader,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):

        data = self.extractor.extract()
        
        if self.transformer:
            data = self.transformer.run(data) 
        for record in data:
            self.loader.run(record)
    
    def __repr__(self) -> str:
        return f'Import job for table: {self.loader.table}.'

            

 


class PipelineTaskSerializer(Protocol):
    def serialize() -> list[Task]:
        ...



class DAGTopologicalSerializer:
    
    def __init__(self, task_predecessor_mapping: dict[Task, set[Task]]):
        self.graph = task_predecessor_mapping

    def serialize(self):
    
        nodes_with_incoming_edges, nodes_without_incoming_edges = self._get_initial_node_lists()
        sorted_nodes = []

        ## Use Kahn's algorithm for topological sort
        while nodes_without_incoming_edges:

            source_node = nodes_without_incoming_edges.pop()
            sorted_nodes.append(source_node)

            nodes_to_update = []
            for node in nodes_with_incoming_edges:
                incoming_node_list = self.graph.get(node)
                if source_node in incoming_node_list:
                    incoming_node_list.remove(source_node)
                    if not incoming_node_list:
                        nodes_to_update.append(node)
                    
            for node in nodes_to_update:
                nodes_with_incoming_edges.remove(node)
                nodes_without_incoming_edges.add(node)
            
        return sorted_nodes
    
    def _get_initial_node_lists(self):
        nodes_without_incoming_edges = {node for node, predecessors in self.graph.items() if not predecessors}
        nodes_with_incoming_edges = set(self.graph.keys()).difference(nodes_without_incoming_edges)
        return nodes_with_incoming_edges, nodes_without_incoming_edges



class PipelineOrchestrator:

    def __init__(self, task_serializer: type[PipelineTaskSerializer] = DAGTopologicalSerializer):
        self.task_serializer = task_serializer
        self.tasks: dict[Task, set[Task]] = {}         
        
    @property
    def task_list(self):
        return self.tasks.keys()
    
    def add_task(self, task: Task, predecessors: set[Task] = set()):
        self.tasks[task] = predecessors


    def run(self):
        serializer = self.task_serializer(self.tasks)
        sorted_task_list = serializer.serialize()
        for task in sorted_task_list:
            task.run()



if __name__ == "__main__":

    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres@localhost/fantasyfootballassistant")
    with Session(engine) as session:

        
        orchestrator = PipelineOrchestrator()

        gw_import = DataImportPipeline(
            extractor = extract.APIExtractor(api.GameWeek, ProjectFiles.gameweeks_json), 
            transformer = tform.GameWeekAdapter(session),
            loader = load.DictionaryDBLoader(Gameweek, session)
        )



        orchestrator.add_task(gw_import)

        orchestrator.run()
        

        







    
    