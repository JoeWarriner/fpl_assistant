from etl.pipeline_management.serializers import PipelineTaskSerializer, DAGTopologicalSerializer
from etl.jobs.base_job import Job


class Pipeline(Job):
    expects_input = False
    task_serializer: type[PipelineTaskSerializer] = DAGTopologicalSerializer

    def __init__(self):
        self.tasks: dict[Job, set[Job]] = {}         
        
    @property
    def task_list(self):
        return self.tasks.keys()
    
    def add_task(self, task: Job, predecessors: set[Job] = set()):
        self.tasks[task] = predecessors


    def run(self):
        serializer = self.task_serializer(self.tasks)
        sorted_task_list = serializer.serialize()
        data = None
        for task in sorted_task_list:
            if task.expects_input: 
                data = task.run(data)
            else:
                data = task.run()
            