from __future__ import annotations
from typing import Protocol
import sqlalchemy
from abc import ABC, abstractmethod
from copy import copy
from sqlalchemy.orm import Session

import etl.update.extracters as extract
import etl.update.api as api
import etl.update.adapters as tform
import etl.update.loaders as load
from etl.update.utils.file_handlers import ProjectFiles
from etl.update.database import Gameweek


class Extracter(Protocol):
   def extract():
       raise NotImplementedError

class Transformer:
    def __init__():
        pass

    def convert():
        raise NotImplementedError

class Loader: 
    def load():
        raise NotImplementedError


class Task(ABC):
    @abstractmethod
    def run(): 
        ...


class DataImportPipeline(Task):
    def __init__(
        self,
        extracter: Extracter,
        transformer: type[Transformer],
        loader: Loader,
    ):
        self.extracter = extracter
        self.transformer = transformer
        self.loader = loader

    def run(self):

        data = self.extracter.extract()
        if self.transformer:
            transformed_data = [self.transformer.convert(item) for item in data]
        for data in transformed_data:
            self.loader.load(data)

 


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

            for node in nodes_with_incoming_edges:
                incoming_node_list = self.graph.get(node)
                if source_node in incoming_node_list:
                    incoming_node_list.remove(source_node)
                    if not incoming_node_list:
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
            extracter = extract.APIExtracter(api.GameWeek, ProjectFiles.gameweeks_json), 
            transformer = tform.GameWeekAdapter(session),
            loader = load.DictionaryDBLoader(Gameweek, session)
        )

        # fixture_import = DataImportPipeline(
        #     extracter = extract.Fixture, 
        #     transformer = tform.GameWeekAdapter(Session),
        #     loader = load.DBLoader()
        # )


        orchestrator.add_task(gw_import)

        orchestrator.run()
        

        







    
    