from etl.pipeline.serializers import PipelineTaskSerializer, DAGTopologicalSerializer
from etl.jobs.base_job import Job


class Pipeline:

    def __init__(self, task_serializer: type[PipelineTaskSerializer] = DAGTopologicalSerializer):
        self.task_serializer = task_serializer
        self.tasks: dict[Job, set[Job]] = {}         
        
    @property
    def task_list(self):
        return self.tasks.keys()
    
    def add_task(self, task: Job, predecessors: set[Job] = set()):
        self.tasks[task] = predecessors


    def run(self):
        serializer = self.task_serializer(self.tasks)
        sorted_task_list = serializer.serialize()
        for task in sorted_task_list:
            task.run()