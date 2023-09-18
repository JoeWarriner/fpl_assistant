from etl.jobs.base_job import Job
from abc import ABC, abstractmethod

JobDependencyMapping = dict[Job, set[Job]]

class JobSerializer(ABC):
    """
    Base serializer class.
    Serializers accept a job dependency mapping object, and return an ordered list of jobs.
    """
    @abstractmethod
    def serialize(self, graph: JobDependencyMapping) -> list[Job]:
        ...


class SimpleSerializer(JobSerializer):
    """
    Serializes dependencies in order added to graph.
    """
    def serialize(self, graph: JobDependencyMapping) -> list[Job]:
        return list(graph.keys())



class TopologicalSerializer(JobSerializer):
    """
    Serializes a dag of jobs/dependencies
    Finds an ordering that meets all dependency requirements.
    """
    def serialize(self, graph:  JobDependencyMapping):
        self.graph = graph
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


